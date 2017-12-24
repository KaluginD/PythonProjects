import json
from copy import copy
from functools import reduce


class ComputeGraph(object):
    """Class for calculations with tables.
    
    Table is a list of dict-like objects without omissions.
    Operations are specified in format of Computing Graph.
    Computing(including reading) occurs separately from specifications.
    Supported operations: Map, Sort, Fold, Reduce, Join.
    
    Public methods:
    __init__(docs, save=None),
    Map(mapper),
    Sort(*args),
    Fold(folder, begin_state),
    Reduce(reducer, *columns),
    Join(on, key, strategy)
    Compute().
    """

    def __init__(self, docs, save=None):
        """Initializing new ComputeGraph.
        
        :param docs: file or another ComputeGraph.
        If file - table will be read from there,
        if ComputeGraph - table will be taken as the result of computing it.
        :param save: file, where result will be written,
        if None - result is not written anywhere.
        """
        self.table = []
        self.docs = docs
        self.save = save
        if isinstance(docs, ComputeGraph):
            self.dependencies = [docs]
        else:
            self.dependencies = []
        self.operations = []
        self.is_computed = False

    def Map(self, mapper):
        """Add Map operation 
        
        :param mapper: generator, which will be called from every table line.
        """
        self.operations.append({'operation': 'map',
                                'args': [mapper]})

    def Sort(self, *args):
        """Add Sort operation
        
        :param args: columns, by which table will be sorted lexicographically.
        """
        self.operations.append({'operation': 'sort',
                                'args': args})

    def Fold(self, folder, begin_state):
        """Add Fold operation.
        
        :param folder: combining function.
        :param begin_state: state to begin.
        """
        self.operations.append({'operation': 'fold',
                                'args': [folder, begin_state]})

    def Reduce(self, reducer, *columns):
        """Add Reduce operation.
        
        :param reducer: generator, which will be called for lines 
        with same value of columns.
        :param columns: columns to group by.
        """
        self.operations.append({'operation': 'reduce',
                                'args': [reducer, *columns]})

    def Join(self, on, key, strategy):
        """Add Join operation.
        
        :param on: another ComputeGraph to join with. 
        It should by computed before begin of execution this join.
        :param key: columns on which tables are joined. 
        Both tables should contain them.
        :param strategy: one of these: 
        ['inner', 'left outer', 'right outer', 'full outer', 'cross'].
        Behavior is similar to SQL Join operation.
        """
        self.dependencies.append(on)
        self.operations.append({'operation': 'join',
                                'args': [on, key, strategy]})

    def Compute(self, verbose=False):
        """Execute all of operations declared earlier(including reading).
        After this, status of graph switches from 'not computed' to 'computed'.
        If calculation this graph requires calculation of other graphs first,
        they will be calculated.
        
        :param verbose: if True, performed operations will be displayed.
        """
        for graph in self.dependencies:
            if not graph.is_computed:
                graph.Compute(verbose)

        if isinstance(self.docs, str):
            with open(self.docs, 'r') as file:
                for line in file.readlines():
                    self.table.append(json.loads(line))
        elif isinstance(self.docs, ComputeGraph):
            self.table = copy(self.docs.table)

        for command in self.operations:
            if verbose:
               print(command['operation'])
            getattr(self, '_' + command['operation'])(*command['args'])
        self.is_computed = True

        if self.save:
            with open(self.save, 'w') as file:
                for line in self.table:
                    file.write(json.dumps(line) + '\n')

    def _map(self, mapper):
        new_table = []
        for line in self.table:
            new_lines = mapper(line)
            if isinstance(new_lines, dict):
                new_table.append(new_lines)
            else:
                for new_line in new_lines:
                    new_table.append(new_line)
        self.table = new_table

    def _sort(self, *args):
        for i, line in enumerate(self.table):
            if isinstance(line, list) or isinstance(line, tuple):
                line = line[0]
            sort_args = [line[i] for i in args]
            self.table[i] = (sort_args, line)
        self.table = sorted(self.table, key=lambda item: item[0])
        self.table = [i[1] for i in self.table]

    def _fold(self, folder, begin_state):
        result = reduce(lambda x, y: folder(x, y), [begin_state] + self.table)
        self.table = [result]

    def _reduce(self, reducer, *columns):
        self._sort(*columns)
        new_table = []

        def columns_equal(i, j):
            flag = True
            for column in columns:
                if self.table[i][column] != self.table[j][column]:
                    return False
            return True

        index = begin = end = 0
        while index < len(self.table):
            begin = index
            index += 1
            while index < len(self.table) and columns_equal(begin, index):
                index += 1
            end = index
            lines = list(reducer(self.table[begin:end]))
            for line in lines:
                if isinstance(line, list):
                    new_table.append(line[0])
                elif isinstance(line, dict):
                    new_table.append(line)
        self.table = new_table

    def _join(self, on, key, strategy='inner'):
        def cross_join(self_table, on_table):
            if not self_table or not on_table:
                return []
            table = list(map(lambda self_line:
                             list(map(lambda on_line:
                                      dict(self_line, **on_line),
                                      on_table)),
                             self_table))
            table = [item for items in table for item in items]
            return table

        if strategy == 'cross':
            self.table = cross_join(self.table, on.table)
            return

        for line in self.table:
            line['__parent_table'] = 'self'
            for tmp_key in key:
                line['__' + tmp_key] = line[tmp_key]
                line.pop(tmp_key, None)

        for line in on.table:
            new_line = copy(line)
            new_line['__parent_table'] = 'on'
            for tmp_key in key:
                new_line['__' + tmp_key] = new_line[tmp_key]
                new_line.pop(tmp_key, None)
            self.table.append(new_line)

        new_keys = ['__' + tmp_key for tmp_key in key]
        self._sort(*new_keys)

        def join_reducer(columns):
            self_lines = []
            on_lines = []
            for tmp_line in columns:
                parent = tmp_line['__parent_table']
                tmp_line.pop('__parent_table', None)
                for tmp_key in key:
                    arg = tmp_line['__' + tmp_key]
                    tmp_line.pop('__' + tmp_key, None)
                    tmp_line[tmp_key] = arg
                if parent == 'self':
                    self_lines.append(tmp_line)
                elif parent == 'on':
                    on_lines.append(tmp_line)
            if strategy in ['full outer', 'right outer'] and not self_lines:
                self_lines.append({self_key: None
                                   for self_key in self.table[0].keys()})
            if strategy in ['full outer', 'left outer'] and not on_lines:
                on_lines.append({on_key: None
                                 for on_key in on.table[0].keys()})
            new_columns = cross_join(self_lines, on_lines)
            for column in new_columns:
                yield column

        self._reduce(join_reducer, *new_keys)
