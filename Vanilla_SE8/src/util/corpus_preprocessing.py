import csv
import os
import sys
from bs4 import BeautifulSoup

sys.path.append('..')
from global_variable import RAW_COURSE_HTML, COURSE_CORPUS, RAW_RETUERS_DIR, REUTERS_CORPUS


def preprocess_course_corpus():
    def _separate_files(src_file):
        end = '</html>'
        with open('../' + src_file, 'r') as f:
            files = f.read().split(end)
        return files

    files = _separate_files(RAW_COURSE_HTML)
    n = 1
    for src_file in files:
        soup = BeautifulSoup(src_file, 'lxml')

        course_name = []
        course_description_lst = []
        doc_id_lst = []

        for e in soup.find_all('div', class_='courseblock'):
            name = e.find('p', class_='courseblocktitle noindent')
            course_description = e.find('p', class_='courseblockdesc noindent')

            if name is not None:
                name = name.text
                if 1 <= int(name[5]) <= 3:  # english course
                    course_name.append(name)

                    # doc_id = 'course_' + str(n)
                    doc_id = n
                    # doc_id = re.sub(' ', '_', name[:8])
                    n += 1

                    doc_id_lst.append(doc_id)

                    if course_description is not None:
                        course_description_lst.append(course_description.text.strip('\n'))
                    else:
                        course_description_lst.append('')

        with open('../' + COURSE_CORPUS, 'a', newline='') as o:
            writer = csv.writer(o)
            for doc_id, name, course_description in zip(doc_id_lst, course_name, course_description_lst):
                writer.writerow([doc_id, name, course_description])


def preprocess_reuters_corpus():
    target_files = [f for f in os.listdir('../' + RAW_RETUERS_DIR) if f.endswith('.sgm')]
    target_files = sorted(target_files)

    # Extract information
    doc_ids = []
    contents = []
    topics_lst = []
    titles = []
    for target_file in target_files:
        with open('../' + RAW_RETUERS_DIR + target_file, 'r', encoding='ISO-8859-1') as f:
            src_file = f.read()
        soup = BeautifulSoup(src_file, 'html.parser')
        for e in soup.find_all('reuters'):
            doc_id = e.get('newid')
            content = e.find('text')
            main_content = (lambda x: x.string if x is not None else '')(content.find('body'))
            topics = (lambda x: list(map(lambda y: y.string, x)) if x != [] else x)(e.find('topics').find_all('d'))
            title = (lambda x: x.string if x is not None else '')(content.find('title'))

            doc_ids.append(doc_id)
            contents.append(main_content.strip())
            topics_lst.append(topics)
            titles.append(title)

    # Write corpus
    with open('../' + REUTERS_CORPUS, 'w', newline='') as o:
        writer = csv.writer(o)
        for doc_id, title, content, topics in zip(doc_ids, titles, contents, topics_lst):
            writer.writerow([doc_id, title, content, topics])


if __name__ == '__main__':
    preprocess_course_corpus()
    preprocess_reuters_corpus()
