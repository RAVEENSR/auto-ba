import string

from nltk.corpus import stopwords
from nltk.stem.porter import *

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize


class TextSimilarityCalculator:
    """
    This class handles text similarity score calculating operations.
    """
    def __init__(self):
        self.tfidf_vectorizer = TfidfVectorizer(analyzer=self.process_text)

    @staticmethod
    def process_text(text):
        """
        Takes a text string and performs:
        1. Removing all punctuation marks
        2. Removing all the stopwords
        and returns a list of the cleaned text
        """
        # Check character by character whether the character is a punctuation
        without_punctuation = [char for char in text if char not in string.punctuation]

        # Form the string by joining characters.
        without_punctuation = ''.join(without_punctuation)

        # Remove any stopwords from the text
        prior_to_stem = [word for word in without_punctuation.split() if word.lower() not in stopwords.words('english')]

        stemmer = PorterStemmer()
        return [stemmer.stem(word) for word in prior_to_stem]

    def cos_similarity(self, string1, string2):
        term_frequency = normalize(self.tfidf_vectorizer.fit_transform([string1, string2]))
        return (term_frequency * term_frequency.T).A[0, 1]

    @staticmethod
    def add_text_similarity_ranking(data_frame):
        data_frame["text_rank"] = data_frame["text_similarity"].rank(method='min', ascending=False)
