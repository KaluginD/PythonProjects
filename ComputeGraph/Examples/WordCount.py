import re

from ComputeGraph import ComputeGraph


def main():
    def tokenizer_mapper(line):
        tokens = line['text'].split()
        for token in tokens:
            word = re.search('([a-zA-Z])+', token)
            if word and len(word.group()) > 0:
                yield {
                    'word': word.group().lower()
                }

    def word_count_reducer(lines):
        yield {
            'word': lines[0]['word'],
            'count': len(lines)
        }

    graph = ComputeGraph.ComputeGraph(docs='data/text_corpus.txt',
                                      save='results/word_count_result.txt')

    graph.Map(tokenizer_mapper)
    graph.Sort('word')
    graph.Reduce(word_count_reducer, 'word')

    graph.Compute()


if __name__ == '__main__':
    main()

