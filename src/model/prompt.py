system_message = """
                      From the given source and target nodes of a directed graph Extract relationships between the nodes
                      Example JSON formatted string Output

                      [{{"source": "John", "target": "US", "relationship" : "Citizen"}},
                      {{"source": "Eifel towel",  "target": "Paris", "relationship" : "travel"}},
                      {{"source": "Hayao Miyazaki",  "target": "Japanese animator", "relationship" : "good"}}]

                      Make sure the output is no longer than 1000 characters in total.
                      ONLY return single word triples and nothing else. None of 'source', 'relationship' and 'target' can be empty.

                      source: \n\n{source} and target: \n\n{target}
                    """

system_message_corrections = """Extract all the relationships between the following entities ONLY based on the give
                Return a list of JSON objects. For example:
                <Examples>
                [{{"source": "John", "target": "US", "relationship" : "Citizen"}},
                {{"source": "Eifel towel",  "target": "Paris", "relationship" : "travel"}},
                {{"source": "Hayao Miyazaki",  "target": "Japanese animator", "relationship" : "good"}}]
                </Examples>
                You have to check or compare example and Answer same type of mistake have done, You have think about mistake and than solve this mistake
                Answer: {Answer}
                - Make sure the corrected output is no longer than 1000 characters in total.
                - ONLY return single word triples and nothing else. None of 'source', 'target' and 'relationship' can be empty.
                - If target is "empty" find target in text "

                source: \n\n{source} and target: \n\n{target}
                """

# system_message_Safety = """
#                         As a caregiver responsible for managing the safety of individuals with Alzheimer's disease,
#                          I need to identify and categorize tags related to safety concerns in this dataset.
#                           Please analyze the provided statements and extract relevant tags that focus on safety issues.
#                         <Examples>
#                         ["Anxiety", "Progression Risk"]
#                         </Examples>
#                         ONLY return list in the specified format. Not more then 10 tages
#                     """

system_message_Safety ="""As a caregiver managing the safety of individuals with Alzheimer's disease, I need a list of concise, 
                          relevant tags that highlight safety concerns from the statements provided. 
                          Focus on identifying specific tags related to risks, behaviors, or environmental factors that could impact safety.

                         Return the extracted tags in the following format, with no more than 10 tags:

                        Example: ["Wandering", "Fall Risk", "Aggression", "Medication Management"]
                        
                        """

system_message_Treatment = """
                      Extract Treatment - Tags from the given stakeholder statement. You to find specific Alzheimer's disease diagnoses tags
                      Return the list object. For example
                      <Examples>
                      ["Medications","Therapies"]
                      </Examples>
                      ONLY return list in the specified format. Not more then 10 tages
                 
                    """

# system_message_diagnosis = """
                      
#                       As a healthcare analyst focused on improving patient outcomes, I need to identify and categorize tags related to diagnosis from this dataset. 
#                       Please analyze the records to find keywords that specifically relate to diagnostic aspects, 
#                       such as types of diagnoses, symptoms, diagnostic tests, or any clinical terms that indicate a diagnosis. 
#                       Tag any terms or references that can provide insights into patient diagnosis information.
#                       Return the list object. For example
#                       <Examples>
#                       ["Medications","Therapies"]
#                       </Examples>
#                       ONLY return list in the specified format. Not more then 10 tages
#                     """

system_message_diagnosis   = """As a healthcare analyst focused on improving patient outcomes, 
                            I need a list of concise, relevant tags that focus specifically on diagnostic aspects from the records provided.
                            Identify keywords or terms that relate directly to diagnoses, 
                            including types of diagnoses, symptoms, diagnostic tests, or any clinical terms that signify diagnosis information.

                            Return the extracted tags in the following format, with no more than 10 tags:

                            Example: ["Disease Types", "Symptom Identification", "Diagnostic Tests", "Clinical Observations"]
                      """

# system_message_Themes = """
#                       From the given Statement of stakehoder. I have extracted Tags from statement, You have to create Theme on bases of Tags like One-liner.
                      
#                       Make sure the output is no longer than 100 characters in total.
#                       ONLY return theme or "Nothing else".
#                       Tags: \n\n{Tags}
#                     """                    

system_message_Themes = """Given a list of extracted tags from the stakeholder's statement,
                          create a single-line theme that concisely summarizes the central focus of the tags.
                            Ensure the theme is relevant to the tags and does not exceed 100 characters.

                          ONLY return the theme.
                          Tags: \n\n{Tags}
                         """



all_tags = """You are analyzing text to extract key tags related to **Safety**, **Treatment**, and **Diagnosis** in the context of Alzheimer's disease. Follow the instructions below:

                    1. **Safety Tags**: Identify risks, behaviors, or environmental factors affecting safety.
                      - Example: ['Wandering', 'Fall Risk', 'Aggression', 'Medication Management']
                      - If no tags are found, return: []

                    2. **Treatment Tags**: Extract terms related to medications, therapies, or interventions.
                      - Example: ['Medications', 'Therapies']
                      - If no tags are found, return: []

                    3. **Diagnosis Tags**: Focus on types of diagnoses, symptoms, diagnostic tests, or clinical observations.
                      - Example: ['Disease Types', 'Symptom Identification', 'Diagnostic Tests', 'Clinical Observations']
                      - If no tags are found, return: []

                    Return the output as three separate lists in the following format:
                Safety: ['tag1', 'tag2', ..., 'tag10']
                Treatment: ['tag1', 'tag2', ..., 'tag10']
                Diagnosis: [tag1', 'tag2', ..., 'tag10']

                **Input Text**: {text}


"""