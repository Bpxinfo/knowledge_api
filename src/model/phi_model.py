from dotenv import load_dotenv
import os
load_dotenv()

key = os.getenv("key")
endpoint = os.getenv("endpoint")

from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage
from azure.core.credentials import AzureKeyCredential


client = ChatCompletionsClient(endpoint=endpoint, credential=AzureKeyCredential(key))