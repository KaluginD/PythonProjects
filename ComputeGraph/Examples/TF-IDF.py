import re
from collections import Counter
from math import log

from ComputeGraph import ComputeGraph


def main():
    split_word = ComputeGraph.ComputeGraph(docs='data/text_corpus.txt')
    count_docs = ComputeGraph.ComputeGraph(docs='data/text_corpus.txt')
    count_idf = ComputeGraph.ComputeGraph(docs=split_word)
    calc_index = ComputeGraph.ComputeGraph(docs=split_word, save='results/tf-idf.txt')

    def tokenizer_mapper(line):
        tokens = line['text'].split()
        for token in tokens:
            word = re.search('([a-zA-Z])+', token)
            if word and len(word.group()) > 4:
                yield {
                    'doc_id': line['doc_id'],
                    'word': word.group().lower()
                }

    split_word.Map(tokenizer_mapper)

    def columns_number_folder(state, _):
        for column in state:
            state[column] += 1
        return state

    count_docs.Fold(columns_number_folder, {'docs_count': 0})

    def unique_columns_reducer(records):
        yield {
            'doc_id': records[0]['doc_id'],
            'word': records[0]['word']
        }

    count_idf.Reduce(unique_columns_reducer, 'doc_id', 'word')
    count_idf.Join(count_docs, [], strategy='outer left')

    def docs_contain_word_reducer(records):
        yield {
            'word': records[0]['word'],
            'docs_contain_word': len(records),
            'docs_count': records[0]['docs_count']
        }

    count_idf.Reduce(docs_contain_word_reducer, 'word')


    def term_frequency_reducer(records):
        word_count = Counter()
        for record in records:
            word_count[record['word']] += 1

        total = sum(word_count.values())
        for word, count in word_count.items():
            yield {
                'doc_id': record['doc_id'],
                'word': word,
                'tf': count / total
            }

    calc_index.Reduce(term_frequency_reducer, 'doc_id')

    calc_index.Join(count_idf, ['word'], strategy='left outer')

    def invert_reducer(records):
        for i, record in enumerate(records):
            records[i]['tf-idf'] = record['tf'] * \
                                   log(record['docs_count'] / record['docs_contain_word'])
        records = sorted(records, reverse=True, key=lambda i: i['tf-idf'])
        top = [tuple([record['doc_id'], record['tf-idf']])
               for record in records[:3]]
        yield {
            'term': records[0]['word'],
            'index': top
        }

    calc_index.Reduce(invert_reducer, 'word')

    calc_index.Compute(True)


if __name__ == '__main__':
    main()
