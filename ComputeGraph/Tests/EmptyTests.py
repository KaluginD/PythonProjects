import pytest
import json

from ComputeGraph import ComputeGraph

def EmptyTests():
    def run():
        test_init()
        test_map_command()
        test_sort_command()
        test_fold_command()
        test_reduce_command()
        test_join_command()

    def test_init():
        graph_to_init = ComputeGraph.ComputeGraph(docs='data/first_table.txt')
        assert graph_to_init.table == []

        graph_to_init.Compute()
        with open('first_table.txt', 'r') as file:
            table = [json.loads(line) for line in file.readlines()]
        assert graph_to_init.table == table

    def test_map_command():
        pass

    def test_sort_command():
        pass

    def test_fold_command():
        pass

    def test_reduce_command():
        pass

    def test_join_command():
        pass