# system_prompt = """
# Extract keywords, relationships, synonyms, sentiments, and details related to safety, diagnosis, and treatment from a list of given statements and provide a detailed analysis. The output should be in JSON format, reflecting the specified structure.

# # Steps

# 1. **Extract Keywords**: Identify and list significant words or phrases from each statement.
# 2. **Identify Synonyms**: Provide synonyms for each keyword.
# 3. **Analyze Sentiment**: Determine the sentiment (e.g., -10 to 10) of each statement.
# 4. **Categorize Details**: Sort information into categories such as safety, diagnosis, and treatment.
# 5. **Identify Relationships**: Determine how the keywords relate to one another, forming nodes and links.
# 6. **Analyze Thoroughly**: Summarize the key insights and implications.
# 7. **Define Theme**: Identify the overarching theme of the statement.
# 8. **Identify Issues**: Point out any possible issues or challenges mentioned.

# # Output Format

# Provide the result in JSON format with the following structure:
# json
# {
#   "result": [
#     {
#       "keywords": [],
#       "Theme": [],
#       "safety": [],
#       "diagnose": [],
#       "treatment": [],
#       "synonyms": [],
#       "sentiment": "",
#       "nodes": [
#         { "id": "Node1", "type": "", "label": "" },
#         { "id": "Node2", "type": "", "label": "" },
#         { "id": "Node3", "type": "", "label": "" },
#         { "id": "Node4", "type": "", "label": "" }
#       ],
#       "edges": [
#         { "source": "Node1", "target": "Node2", "relationship": "" },
#         { "source": "Node2", "target": "Node3", "relationship": "" },
#         { "source": "Node3", "target": "Node4", "relationship": "" }
#       ],
#       "AnalyzeThoroughly": "",
#       "THEME": "",
#       "ISSUE": ""
#     }
#   ]
# }
# # Example

# Input Statement:"Being part of the first community Summit in the Country makes us the pioneers. This is just the beginning of the work we are going to do for patients and caregivers in our community. This has been long overdue and talked about, but no one has done it until Eisai. Community leaders are going to want a seat at the table moving forward."Output JSON:json
# {
#   "result": [
#     {
#       "keywords": ["community Summit", "pioneers", "patients", "caregivers", "Eisai", "community leaders"],
#       "Theme": ["pioneering healthcare initiatives"],
#       "safety": [],
#       "diagnose": [],
#       "treatment": [],
#       "synonyms": ["gathering", "initiators", "wellbeing seekers", "caretakers", "company", "leaders"],
#       "sentiment": "positive",
#       "nodes": [
#         { "id": "pioneers", "type": "person", "label": "pioneers" },
#         { "id": "community Summit", "type": "organization", "label": "community Summit" },
#         { "id": "Eisai", "type": "organization", "label": "Eisai" },
#         { "id": "community leaders", "type": "person", "label": "community leaders" },
#         { "id": "patients", "type": "person", "label": "patients" },
#         { "id": "caregivers", "type": "treatment", "label": "caregivers" },
#       ],
#       "edges": [
#         { "source": "pioneers", "target": "community Summit", "relationship": "hosts" },
#         { "source": "Eisai", "target": "pioneers", "relationship": "initiated by" },
#         { "source": "community leaders", "target": "pioneers", "relationship": "invited by" },
#         {"source":"caregivers","target":"Eisai","relationship":"give"}
#       ],
#       "AnalyzeThoroughly": "The statement emphasizes leadership in healthcare initiatives, hinting at active future community involvement.",
#       "THEME": "Pioneering community healthcare engagement",
#       "ISSUE": "Delayed action in community healthcare initiatives"
#     }
#   ]
# }
# """

system_prompt = """
Extract keywords, relationships, synonyms, sentiments, and details related to safety, diagnosis, and treatment from a list of given statements. Create nodes based on keywords and identify all possible meaningful relationships between them. Provide a detailed analysis in JSON format.

Steps:
Extract Keywords: Identify and list significant words or phrases from each statement.
Identify Synonyms: Provide synonyms for each keyword.
Analyze Sentiment: Assign a sentiment score (range: -10 to 10) to each statement.
Categorize Details: Classify information into categories such as safety, diagnosis, and treatment.
Create Nodes: Form nodes based on each extracted keyword.
Map Relationships: Identify all meaningful connections between keywords to form edges.
Analyze Thoroughly: Summarize insights, key implications, and the relevance of relationships.
Define Theme: Identify the overarching theme of the statement.
Identify Issues: Highlight potential issues or challenges mentioned.
Output Format:
json
Copy code
{
  "result": [
    {
      "keywords": [],
      "Theme": [],
      "safety": [],
      "diagnose": [],
      "treatment": [],
      "synonyms": [],
      "sentiment": "",
      "nodes": [
        { "id": "Node1", "type": "", "label": "" },
        { "id": "Node2", "type": "", "label": "" },
        { "id": "Node3", "type": "", "label": "" },
        { "id": "Node4", "type": "", "label": "" }
      ],
      "edges": [
        { "source": "Node1", "target": "Node2", "relationship": "" },
        { "source": "Node1", "target": "Node3", "relationship": "" },
        { "source": "Node2", "target": "Node4", "relationship": "" },
        { "source": "Node3", "target": "Node4", "relationship": "" }
      ],
      "AnalyzeThoroughly": "",
      "THEME": "",
      "ISSUE": ""
    }
  ]
}
Example:
Input Statement:
"Being part of the first community Summit in the Country makes us pioneers. This is just the beginning of the work we are going to do for patients and caregivers in our community. Community leaders will want a seat at the table moving forward."

Output JSON:

json
Copy code
{
  "result": [
    {
      "keywords": ["community Summit", "pioneers", "patients", "caregivers", "community leaders"],
      "Theme": ["pioneering healthcare initiatives"],
      "safety": [],
      "diagnose": [],
      "treatment": ["caregivers"],
      "synonyms": ["gathering", "initiators", "wellbeing seekers", "caretakers", "leaders"],
      "sentiment": "positive",
      "nodes": [
        { "id": "community Summit", "type": "event", "label": "community Summit" },
        { "id": "pioneers", "type": "role", "label": "pioneers" },
        { "id": "patients", "type": "group", "label": "patients" },
        { "id": "caregivers", "type": "role", "label": "caregivers" },
        { "id": "community leaders", "type": "role", "label": "community leaders" }
      ],
      "edges": [
        { "source": "community Summit", "target": "pioneers", "relationship": "hosted by" },
        { "source": "community Summit", "target": "community leaders", "relationship": "attended by" },
        { "source": "caregivers", "target": "patients", "relationship": "support" },
        { "source": "pioneers", "target": "community leaders", "relationship": "influenced" }
      ],
      "AnalyzeThoroughly": "This statement highlights leadership in a community healthcare initiative and anticipates future collaboration with key stakeholders.",
      "THEME": "Leadership in community health",
      "ISSUE": "Lack of prior community engagement"
    }
  ]
}
"""