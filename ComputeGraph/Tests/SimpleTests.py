import json
from copy import copy

import unittest
import pytest

from ComputeGraph import ComputeGraph

class SimpleTests(unittest.TestCase):

    def test_init(self):
        with open('data/first_table.txt', 'r') as file:
            table = [json.loads(line) for line in file.readlines()]

        graph_to_init = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        self.assertEqual(graph_to_init.table, [])

        graph_to_init.Compute()
        self.assertEqual(graph_to_init.table, table)

    def test_map_command(self):
        with open('data/first_table.txt', 'r') as file:
            table_to_map = [json.loads(line) for line in file.readlines()]

        def simple_mapper(line):
            line['hex'], line['rgb'] = list(reversed(line['rgb'])), ''.join(sorted(line['hex']))
            yield line

        graph_to_map = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        graph_to_map.Map(simple_mapper)
        graph_to_map.Compute()

        table_to_map = [item for line in table_to_map for item in simple_mapper(line)]
        self.assertEqual(graph_to_map.table, table_to_map)

    def test_sort_command(self):
        with open('data/first_table.txt', 'r') as file:
            table_to_sort = [json.loads(line) for line in file.readlines()]

        graph_to_sort = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        graph_to_sort.Sort('hex', 'rgb')
        graph_to_sort.Compute()

        table_to_sort = sorted(table_to_sort, key=lambda item: (item['hex'], item['rgb']))
        self.assertEqual(graph_to_sort.table, table_to_sort)

    def test_fold_command(self):
        with open('data/first_table.txt', 'r') as file:
            table_to_fold = [json.loads(line) for line in file.readlines()]

        def simpe_folder(state, record):
            for column in state:
                state[column] += record[column]
            return state

        graph_to_fold = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        graph_to_fold.Fold(simpe_folder, {'hex' : ''})
        graph_to_fold.Compute()
        table_to_fold = [{'hex' : ''.join([line['hex'] for line in table_to_fold])}]
        self.assertEqual(graph_to_fold.table, table_to_fold)


    def test_reduce_command(self):
        with open('data/first_table.txt', 'r') as file:
            table_to_reduce = [json.loads(line) for line in file.readlines()]

        def simple_reducer(columns):
            for column in columns:
                column['hex'], column['rgb'] = list(reversed(column['rgb'])), ''.join(sorted(column['hex']))
                yield column

        graph_to_reduce = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        graph_to_reduce.Reduce(simple_reducer, 'color')
        graph_to_reduce.Compute()

        table_to_reduce = sorted([copy(line) for line in table_to_reduce], key=lambda item: item['color'])
        table_to_reduce = [item for line in table_to_reduce for item in simple_reducer([line])]
        self.assertEqual(graph_to_reduce.table, table_to_reduce)


    def test_join_command(self):
        with open('data/first_table.txt', 'r') as file:
            table_to_join = [json.loads(line) for line in file.readlines()]

        graph_to_join = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        graph_to_join_on = ComputeGraph.ComputeGraph(docs='data/second_table.txt')
        graph_to_join.Join(graph_to_join_on, ['color'], 'inner')
        graph_to_join.Compute()

        table_to_join = sorted([copy(line) for line in table_to_join], key=lambda item: item['color'])
        table_to_join_on = []
        with open('data/second_table.txt', 'r') as file:
            for line in file.readlines():
                table_to_join_on.append(json.loads(line))
        for line in table_to_join:
            for line_on in table_to_join_on:
                if line['color'] == line_on['color']:
                    line.update(line_on)
        self.assertEqual(graph_to_join.table, table_to_join)