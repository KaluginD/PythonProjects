from collections import Counter

import ComputeGraph


def main():
    '''
    self_table = [{'q':1, 'w':2}]
    on_table = [{'r':3, 't':4}]
    table = list(map(lambda self_line:
                     list(map(lambda on_line:
                              dict(self_line, **on_line),
                              on_table)),
                     self_table))
    table = [item for items in table for item in items]
    print(table)

    def sum_columns_folder(state, record):
        for column in state:
            state[column] += record[column] + ' '
        return state

    def mapper(r):
        yield {'words': len(r['text'].split())}

    graph = ComputeGraph.ComputeGraph()



    print('SORTING:')
    graph.Sort('word', 'doc_id')
    for i in graph.table:
        print(i)

    graph.Compute(docs='data/short_text.txt', save='results/text_result.txt')
    print(graph.table)
    def sum_columns_folder(state, record):
        for column in state:
            state[column] += record[column] + ' '
        return state

    graph.Fold(sum_columns_folder, {'word': ''})

    graph.Fold(sum_columns_folder, {'text': ''})
    graph.Map(mapper)

    print('FOLDING:')
    def sum_columns_folder(state, record):
        for column in state:
            state[column] += record[column]
        return state

    folding = graph.Fold(sum_columns_folder, {'word': ''})
    print(folding)

    print('REDUCING:')
    def term_frequency_reducer(records):
        word_count = Counter()
        for r in records:
            word_count[r['word']] += 1

        total = sum(word_count.values())
        for w, count in word_count.items():
            yield {
                'doc_id' : r['doc_id'],
                'word' : w,
                'tf' : count / total
            }
    graph.Reduce(term_frequency_reducer, 'word')
    for i in graph.table:
        print(i)
    '''

    graph_to_join = ComputeGraph.ComputeGraph()
    graph_to_join.Compute('data/text_to_join.txt')


    print('JOINING:')

    graph = ComputeGraph.ComputeGraph()

    graph.Sort('text')

    graph.Join(graph_to_join, key=(['text'],['word1']), strategy='right outer')

    graph.Compute('data/short_text.txt')
    print('computed!')
    for i in graph.table:
        print(i)

if __name__ == '__main__':
    main()



"""
    def join_(self, on, key, strategy='inner'):
        self.Sort(*key)
        on.Sort(*key)
        new_table = []
        def compare(first, second):
            for k in key:
                if on.table[first][k] < self.table[second][k]:
                    return True
            return False

        def equal(first, second):
            return not compare(first, second) and not compare(second, first)

        if strategy == 'inner':
            for self_index in range(len(self.table)):
                on_index = 0
                while on_index < len(on.table) and compare(on_index, self_index):
                    on_index += 1
                while on_index < len(on.table) and equal(self_index, on_index):
                    new_line = copy(self.table[self_index])
                    for key_, value_ in on.table[on_index].items():
                        new_line[key_] = value_
                    new_table.append(new_line)
                    on_index += 1
            self.table = new_table

        if 'outer' in strategy:
            if 'left' in strategy:
                for self_index in range(len(self.table)):
                    on_index = 0
                    flag = True
                    while on_index < len(on.table) and compare(on_index, self_index):
                        on_index += 1
                    while on_index < len(on.table) and equal(self_index, on_index):
                        flag = False
                        new_line = copy(self.table[self_index])
                        for key_, value_ in on.table[on_index].items():
                            new_line[key_] = value_
                        new_table.append(new_line)
                        on_index += 1
                    if flag:
                        new_line = copy(self.table[self_index])
                        for key_, value_ in on.table[min(on_index, len(on.table) - 1)].items():
                            new_line[key_] = None
                        new_table.append(new_line)
                return new_table

            if 'right' in strategy:
                for on_index in range(len(on.table)):
                    self_index = 0
                    flag = True
                    while self_index < len(self.table) and compare(self_index, on_index):
                        self_index += 1
                    while self_index < len(self.table) and equal(self_index, on_index):
                        flag = False
                        new_line = copy(on.table[on_index])
                        for key_, value_ in self.table[self_index].items():
                            new_line[key_] = value_
                        new_table.append(new_line)
                        self_index += 1
                    if flag:
                        new_line = copy(self.table[on_index])
                        for key_, value_ in self.table[min(self_index, len(self.table) - 1)].items():
                            new_line[key_] = None
                        new_table.append(new_line)
                return new_table

            if 'full' in strategy:
                self_unused = list(range(len(self.table)))
                on_unused = list(range(len(on.table)))
                for self_index in range(len(self.table)):
                    on_index = 0
                    while on_index < len(on.table) and compare(on_index, self_index):
                        on_index += 1
                    while on_index < len(on.table) and equal(self_index, on_index):
                        new_line = copy(self.table[self_index])
                        for key_, value_ in on.table[on_index].items():
                            new_line[key_] = value_
                        new_table.append(new_line)
                        on_index += 1
                        if self_index in self_unused:
                            self_unused.remove(self_index)
                        if on_index in on_unused:
                            on_unused.remove(on_index)
                for self_index in self_unused:
                    new_line = copy(self.table[self_index])
                    for key_, value_ in on.table[0].items():
                        new_line[key_] = None
                    new_table.append(new_line)
                for on_index in on_unused:
                    new_line = copy(on.table[on_index])
                    for key_, value_ in self.table[0].items():
                        new_line[key_] = None
                    new_table.append(new_line)
                self.table = new_table

        if strategy == 'cross':
            for self_index in range(len(self.table)):
                for on_index in range(len(on.table)):
                    new_line = copy(self.table[self_index])
                    for key_, value_ in on.table[on_index].items():
                        new_line[key_] = value_
                    new_table.append(new_line)
            self.table = new_table
"""