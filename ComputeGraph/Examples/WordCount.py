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

    count_docs = ComputeGraph.ComputeGraph(docs='data/text_corpus.txt')

    def columns_number_folder(state, _):
        for column in state:
            state[column] += 1
        return state

    count_docs.Fold(columns_number_folder, {'docs_count': 0})


    def tokenizer_mapper(line):
        tokens = line['text'].split()
        for token in tokens:
            word = re.search('([a-zA-Z])+', token)
            if word and len(word.group()) > 4:
                yield {
                    'doc_id': line['doc_id'],
                    'word': word.group().lower()
                }

    graph.Map(tokenizer_mapper)

    graph.Join(count_docs, (), 'left outer')

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

    graph.Reduce(clean_reducer, 'word')



    # graph.Map(tokenizer_mapper)
    graph.Sort('word')
    graph.Reduce(word_count_reducer, 'word')

    graph.Compute()

    for i in graph.table:
        if i['word'] in ['looked', 'carried', 'night', 'enough', 'better',
                          'under', 'young', 'never', 'another', 'people']:
            print(i)


if __name__ == '__main__':
    main()
