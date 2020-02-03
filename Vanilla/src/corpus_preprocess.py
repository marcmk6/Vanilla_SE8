from bs4 import BeautifulSoup
import csv
import re

src = '../UofO_Courses.html'
out = '../course_corpus.csv'


def separate_files(src_file):
    end = '</html>'
    with open(src_file, 'r') as f:
        files = f.read().split(end)

    return files


def preprocess(src_file):
    soup = BeautifulSoup(src_file, 'lxml')

    course_name = []
    course_description = []
    doc_id = []

    for e in soup.find_all('div', class_='courseblock'):
        name = e.find('p', class_='courseblocktitle noindent')
        description = e.find('p', class_='courseblockdesc noindent')

        if name is not None:
            name = name.text
            if 1 <= int(name[5]) <= 3:  # english course
                course_name.append(name)

                # docid = name[:8]
                docid = re.sub(' ', '_', name[:8])
                doc_id.append(docid)

                if description is not None:
                    course_description.append(description.text.strip('\n'))
                else:
                    course_description.append('')

    with open(out, 'a') as o:
        writer = csv.writer(o)
        for docid, name, description in zip(doc_id, course_name, course_description):
            writer.writerow([docid, name, description])


if __name__ == "__main__":
    files = separate_files(src)
    # for file in files:
    #     preprocess(file)
    preprocess(files[3])
