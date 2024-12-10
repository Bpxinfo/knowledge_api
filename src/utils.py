import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt_tab')
from nltk.stem import WordNetLemmatizer
import os
from typing import List
lemmatizer = WordNetLemmatizer()
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))
import pandas as pd


import json
from datetime import datetime


# Custom encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO 8601 string
        return super().default(obj)


import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.exception import CustomException



def word_preprocess(text):
    tokens = nltk.word_tokenize(text.lower())
    tokens = [word for word in tokens if word.isalpha() and word not in stop_words]
    return ' '.join(tokens)

    

def save_json(data,file_path):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4, cls=DateTimeEncoder)
    except Exception as e:
        raise CustomException(e, sys)
    

def format_entities(ent_list: List[str]) -> str:
    return "\n\n".join([e for e in ent_list])    

def extract_source_target(data):
    sources = []  # List to hold source names
    targets = []  # List to hold target names

    for group in data:
        # for entry in group:
          source = group.get('source')
          target = group.get('target')
          if source is not None:  # Check if source exists
              sources.append(source)
          if target is not None:  # Check if target exists
              targets.append(target)

    return sources, targets

def find_json_data(text):
    # Search for the first '[' and the last ']'
    match = re.search(r'\[.*\]', text, re.DOTALL)

    if match:
        return match.group(0)
    return None



def remove_stopwords(text,max_stopwords=10):
    """
    Remove stopwords from a given text and return the cleaned text and identified stopwords.

    Args:
        text (str): Input text.

    Returns:
        tuple: (cleaned_text, stopwords_in_text)
    """
    # Handle None or empty strings
    if not isinstance(text, str) or not text.strip():
        return "", []

    # Standardize stopwords
    stop_words = set(stopwords.words('english'))

    # Preprocessing: lowercase, remove punctuation, extra whitespace
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()

    # Tokenize
    words = text.split()

    # Separate stopwords and non-stopwords
    text_stopwords = [word for word in words if word in stop_words]
    # Limit the number of stopwords if max_stopwords is provided
    if max_stopwords is not None:
        text_stopwords = text_stopwords[:max_stopwords]

    cleaned_text = ' '.join(word for word in words if word not in stop_words)

    return cleaned_text, text_stopwords

def extract_keywords(text, top_n=10):
    """
    Extract top N keywords from a given text using TF-IDF.

    Args:
        text (str): Input text.
        top_n (int): Number of top keywords to extract.

    Returns:
        list: List of top N keywords.
    """
    # Handle None or empty strings
    if not text.strip():
        return []

    # TF-IDF for keywords
    try:
        tfidf = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf.fit_transform([text])
        feature_names = tfidf.get_feature_names_out()
        tfidf_scores = tfidf_matrix.toarray()[0]

        # Get top N keywords based on TF-IDF scores
        if len(feature_names) > 0:
            top_indices = tfidf_scores.argsort()[-min(top_n, len(feature_names)):][::-1]
            keywords = [feature_names[i] for i in top_indices]
        else:
            keywords = []

        return keywords

    except ValueError:
        # Fallback if TF-IDF fails
        return []
    


import re
import json

def extract_medical_lists(text):

     # Extract JSON content between first { and last }
    match = re.search(r'\{.*\}', text, re.DOTALL)
    
    if not match:
        print("No JSON content found")
        return None
    
    # Get the matched JSON string
    json_str = match.group(0)
    # Parse the JSON text
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        print("Invalid JSON format")
        return None
    
    # Check if the result is a list and has at least one item
    if not data.get('result') or not isinstance(data['result'], list):
        print("No result found in the JSON")
        return None
    
    # Get the first result item (assuming single result)
    result = data['result'][0]
    
    # Create medical lists dictionary
    medical_lists = {
        'keywords': result.get('keywords', []),
        'Theme': result.get('Theme', []) or result.get('THEME', []),
        'Safety': result.get('safety', []),
        'Treatment': result.get('treatment', []),
        'Diagnosis': result.get('diagnose', []),
        'sentiment': [result.get('sentiment', '')],
        'nodes': result.get('nodes', []),
        'edges': result.get('edges', []) or result.get('Edges', []),
        'AnalyzeThoroughly': [result.get('AnalyzeThoroughly', '')],
        'Issue': result.get('ISSUE', []),
        'synonyms': result.get('synonyms', {})
    }
    
    return medical_lists
