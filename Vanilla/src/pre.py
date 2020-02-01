from bs4 import BeautifulSoup
import csv

src = '../UofO_Courses.html'
out = '../course_corpus.csv'

with open(src, 'r') as f:
	soup = BeautifulSoup(f, 'lxml')

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

			docid = name[:8]
			doc_id.append(docid)

			if description is not None:
				course_description.append(description.text.strip('\n'))
			else:
				course_description.append('')

with open(out, 'w') as o:
	writer = csv.writer(o)
	for docid, name, description in zip(doc_id, course_name, course_description):
		writer.writerow([docid, name, description])