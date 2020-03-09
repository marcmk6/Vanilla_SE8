from intermediate_class.index_configuration import __get_all_possible_index_configurations__

"""Dedicated directory"""
INDEX_DIR = '../index/'
CORPUS_DIR = '../corpus/'

"""Corpus"""
RAW_COURSE_HTML = CORPUS_DIR + 'UofO_Courses.html'
RAW_RETUERS_DIR = CORPUS_DIR + 'reuters21578/'
COURSE_CORPUS = CORPUS_DIR + 'course_corpus.csv'
REUTERS_CORPUS = CORPUS_DIR + 'reuters_corpus.csv'
TMP_AVAILABLE_CORPUS = {'course_corpus': COURSE_CORPUS, 'Reuters': REUTERS_CORPUS}

"""File extension"""
INDEX_FILE_EXTENSION = '.idx'
CORPUS_FILE_EXTENSION = '.csv'
BIGRAM_LANGUAGE_MODEL_EXTENSION = '.blm'

"""Index configuration"""
ALL_POSSIBLE_INDEX_CONFIGURATIONS = __get_all_possible_index_configurations__()
TMP_ALL_POSSIBLE_INDEX_CONF_TUPLES = [tuple(e for e in ALL_POSSIBLE_INDEX_CONFIGURATIONS)]

"""Retrieval model"""
BOOLEAN_MODEL = 'boolean'
VSM_MODEL = 'vsm'
QUERY_MODELS = {VSM_MODEL, BOOLEAN_MODEL}

"""VSM retrieval specific"""
DOC_RETRIEVAL_LIMIT = 10

"""Boolean retrieval specific"""
DUMMY_WORD = 'DUMMY_WORD'

"""Spelling correction"""
UNFOUND_TERM_LIMIT = 3

"""Bigram language model - Query completion"""
BLM_THRESHOLD = 5

