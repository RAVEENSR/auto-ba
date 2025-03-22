import string
import nltk

from nltk.corpus import stopwords
from nltk.stem.porter import *

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

try:
    stopwords.words('english')
except LookupError:
    nltk.download('stopwords')

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
        if not text:
            return []  # Safely return empty if input is None or empty

        # Check character by character whether the character is a punctuation
        without_punctuation = [char for char in text if char not in string.punctuation]

        # Form the string by joining characters.
        without_punctuation = ''.join(without_punctuation)

        # Remove any stopwords from the text
        prior_to_stem = [word for word in without_punctuation.split()
                         if word.lower() not in stopwords.words('english')]

        stemmer = PorterStemmer()
        return [stemmer.stem(word) for word in prior_to_stem]

    def cos_similarity(self, string1, string2):
        if not string1 or not string2:
            return 0.0  # No similarity if one is missing

        processed1 = self.process_text(string1)
        processed2 = self.process_text(string2)

        # If after preprocessing, there's no content left
        if not processed1 or not processed2:
            return 0.0

        try:
            term_frequency = normalize(self.tfidf_vectorizer.fit_transform([string1, string2]))
            return (term_frequency * term_frequency.T).A[0, 1]
        except ValueError:
            # Catch edge cases like completely empty TF matrix
            return 0.0

    @staticmethod
    def add_text_similarity_ranking(data_frame):
        data_frame["text_rank"] = data_frame["text_similarity"].rank(method='min', ascending=False)
