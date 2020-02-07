import csv
import text_processing


# Module 3 - Dictionary building
def build_vocabulary(corpus, stop_words_removal=True, stemming=True, normalization=True):
    terms = set()
    with open(corpus, 'r') as corpus_file:
        reader = csv.reader(corpus_file)
        for row in reader:
            doc_id = row[0]
            title = row[1]
            content = row[2]

            # to_be_processed = [title, description]
            to_be_processed = [title + content]

            for e in to_be_processed:
                processed_tokens = text_processing.process(e, stop_words_removal=stop_words_removal, stemming=stemming,
                                                           normalization=normalization)
                for term in processed_tokens:
                    terms.add(term)

    return terms


if __name__ == "__main__":
    corpus = '../course_corpus.csv'
    print(build_vocabulary(corpus))
