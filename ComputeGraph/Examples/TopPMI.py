import re
from collections import Counter
from math import log

from ComputeGraph import ComputeGraph


def main():
    count_docs = ComputeGraph.ComputeGraph(docs='data/text_corpus.txt')
    split_word = ComputeGraph.ComputeGraph(docs='data/text_corpus.txt')
    count_words = ComputeGraph.ComputeGraph(docs=split_word)
    gf = ComputeGraph.ComputeGraph(docs=split_word)
    pmi = ComputeGraph.ComputeGraph(docs=split_word, save='results/pmi.txt')

    # Count columns in table(to cleanup excess words later)

    def columns_number_folder(state, _):
        for column in state:
            state[column] += 1
        return state

    count_docs.Fold(columns_number_folder, {'docs_count': 0})

    # Split words

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

    # Clean words

    split_word.Join(count_docs, (), 'left outer')

    def clean_reducer(records):
        doc_number = records[0]['docs_count']
        doc_dict = {}
        for record in records:
            if record['doc_id'] not in doc_dict:
                doc_dict[record['doc_id']] = 1
            else:
                doc_dict[record['doc_id']] += 1
        flag = len(doc_dict.keys()) >= doc_number
        for value in list(doc_dict.values()):
            if value < 2:
                flag = False
        if flag:
            for record in records:
                yield {
                    'doc_id': record['doc_id'],
                    'word': record['word']
                }

    split_word.Reduce(clean_reducer, 'word')

    # Count frequency of word_i in all documents combined

    def words_number_folder(state, _):
        for column in state:
            state[column] += 1
        return state

    gf.Fold(words_number_folder, {'words_count': 0})

    gf.Join(count_words, (), 'left outer')

    def gf_mapper(columns):
        yield {
            'word': columns[0]['word'],
            'gf': len(columns) / columns[0]['words_count']
        }

    gf.Reduce(gf_mapper, 'word')

    # Count frequency of word_i in doc_i

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

    pmi.Reduce(term_frequency_reducer, 'doc_id')

    # Count pmi

    pmi.Join(gf, ['word'], 'left outer')

    def pmi_mapper(line):
        curr_pmi = log(line['tf'] / line['gf'])
        yield {
            'doc_id': line['doc_id'],
            'word': line['word'],
            'pmi': curr_pmi
        }

    pmi.Map(pmi_mapper)

    # Find top words for each file

    def top_pmi_reducer(records):
        top_records = sorted(records, reverse=True, key=lambda item: item['pmi'])[:10]
        index = [tuple([record['word'], record['pmi']]) for record in top_records]
        yield {
            'doc_id': records[0]['doc_id'],
            'index': index
        }

    pmi.Reduce(top_pmi_reducer, 'doc_id')

    pmi.Compute()


if __name__ == '__main__':
    main()
