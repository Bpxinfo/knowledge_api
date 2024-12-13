import os
import sys
import json
from flask import Flask, jsonify, request, render_template
import pandas as pd
from flask_cors import CORS
import asyncio
# Adjust path to locate modules in src directory
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# from src.components.knowledge_graph import GraphAnalyzer
from src.components.data_ingestion import DataIngestion
from src.components.data_transformation import DataTransformation
from src.components.Extract_and_save import Relation_extraction
from src.db.new_flsk import get_table_names, fetch_data_from_table,get_sheet_names_from_db,fetch_table_data,fetch_edges
from src.db.curd import  sql_add_node,sql_add_connection,sql_delete_record_nodes,sql_delete_record_edges, merge_edges, merge_nodes_data, merge_multiple_edges, merge_multiple_nodes_data,get_node_by_geo,get_scatter_data,get_timeline_data, get_stakeholder,Tree_map, advance_search
from src.db.Create_user_upload_sheet import retrieve_user_data,is_sheet_name_table_exists,create_sheet_name_table
from src.components.load_database import load_into_db
from src.components.New_clean_json import Extract_clean_json

obj = DataIngestion()
data_transformation= DataTransformation()
extract_all = Relation_extraction()
clean_json = Extract_clean_json()
load_into_database = load_into_db()
# analyzer = GraphAnalyzer()

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return "Successfully Deployed"

# @app.route('/home')
# def home():
#     return render_template('index_2.html')

# @app.route('/login', methods=['POST'])
# def login():
#     pass

# @app.route('/logout')
# def logout():
#     pass

# @app.sign_in('/sign_in', methods = ['POST'])
# def sign_in():
#     pass


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    # Validate file type
    if not file.filename.lower().endswith(('.xls', '.xlsx')):
        return jsonify({'error': 'Invalid file type. Please upload an Excel file.'}), 400
    
    # Save the file temporarily
    try:
        df = pd.read_excel(file, sheet_name=None)  # Read all sheets
        sheet_names = list(df.keys())  # Get sheet names
        return jsonify({
            'sheets': sheet_names,
            # 'total_sheets': len(sheet_names)
        })
    except ValueError as ve:
        return jsonify({'error': 'Error reading Excel file: ' + str(ve)}), 400
    except Exception as e:
        return jsonify({'error': 'Unexpected error processing file: ' + str(e)}), 500


# Route to get columns of the selected sheet
@app.route('/get_columns', methods=['POST'])
def get_columns():
    sheet_name = request.form.get('sheet_name')  # Get sheet name from form data
    file = request.files['file']  # Get file from the request
    
    try:
        # Read the specified sheet from the uploaded Excel file
        df = pd.read_excel(file, sheet_name=sheet_name)
        columns = df.columns.tolist()
        return jsonify({'columns': columns})
    except Exception as e:
        return jsonify({'error': f"Error processing sheet {sheet_name}: {str(e)}"}), 500


@app.route('/get_column_data', methods=['POST'])
def get_column_data():
    try:
        # Get the file, metadata (target column), sheet name from request
        file = request.files['file']
        metadata = request.form.get('metadata', '')  # Using .get() to avoid KeyError if 'metadata' is missing
        sheet_name = request.form.get('sheet_name', '')
        column_name = json.loads(request.form.get('column_names', '[]'))

        # print(f"Received sheet_name: {sheet_name}")
        # print(f"Received sheet_name: {type(sheet_name)}")
        # print(f"Received sheet_name: {metadata}")

        if not metadata or not sheet_name:
            return jsonify({'error': 'Missing required fields (metadata or sheet_name)'}), 400
 
        if not is_sheet_name_table_exists():
            create_sheet_name_table()

        if retrieve_user_data(sheetname=str(sheet_name)) == "True":

            feedback_path = obj.initiate_data_ingestion(data=file, sheetname=sheet_name)
            Transforme_data_path = data_transformation.initiate_Data_transformation(feedback_path,target_columns=metadata,columnsname=column_name)  
            Clean_json_extraction = clean_json.initiate_json(data_path=Transforme_data_path,target_column=metadata)

            extracted_records = asyncio.run(extract_all.load_and_extract(
                data_path=Clean_json_extraction,
                target_column=metadata,)
            )
            load_data = asyncio.run(load_into_database.load_and_database(
                data_path=extracted_records, 
                sheet_name=sheet_name,
            ))

            return jsonify({'msg':"Succesfully created and insert data"})
    
        else:
            return jsonify({'msg':'Sheetname already exist'})

        # return jsonify({'feedback_path': feedback_path})

    except Exception as e:
        return jsonify({'error': f"Error processing the request: {str(e)}"}), 500



#----------------------------------------------------------------------
# Get data from sheet table rander in fornt page
@app.route('/get-sheet-names', methods=['GET'])
def get_sheet_names():
    """
    API endpoint to retrieve data from the Sheet_name table.
    Calls the get_sheet_names_from_db function.
    """
    try:
        result = get_sheet_names_from_db()
        status_code = 200 if result['status'] == 'success' else 404
        return jsonify(result), status_code

    except Exception as e:
        app.logger.error(f"Error in get_sheet_names endpoint: {e}")
        return jsonify({
            "message": "An internal server error occurred.",
            "status": "error",
            "error": str(e)
        }), 500


@app.route('/totaluploadData', methods=['GET'])
def list_tables():
    tables = get_table_names()
    print(tables)
    if isinstance(tables, list):
        return jsonify({"data": tables})
    else:
        return jsonify({"data": tables}), 500



@app.route('/data', methods=['GET'])
def get_data():
    table_name = request.args.get('table')  # Get table name from query parameters
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    data = fetch_data_from_table(table_name)
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 500

    return jsonify({"data": data})



# API route to fetch data
@app.route('/fetch-data', methods=['POST'])
def fetch_data():
    try:
        # Get data from the request
        data = request.get_json()

        if not data.get('sheet_name') or not data.get('id'):
            raise ValueError("Both 'sheet_name' and 'id' are required")
        
        sheet_name = data['sheet_name']
        id_value = data['id']
        
        # Fetch data from the table
        result = fetch_table_data(sheet_name, id_value)
        
        # Handle no records found
        if not result:
            return jsonify({'message': 'No matching records found'}), 404
        
        return jsonify({'data': result}), 200
    
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f"An unexpected error occurred: {str(e)}"}), 500


# Get knowledge graph data
@app.route('/fatch_edges', methods = ['POST'])
def get_edges():
    try:
        data =  request.get_json('sheet_name')
        sheet_name = data['sheet_name']
        edge_data, node_data = fetch_edges(sheet_name)
        dataaa = {'edges': edge_data,
                        'nodes':node_data}
        # result = analyzer.calculate_nodes(dataaa)

        # Handle no records found
        # if not result:
        #     return jsonify({'message': 'No matching records found'}), 404
        
        return jsonify({'edges': edge_data,
                        'nodes':node_data}), 200
    
        # return jsonify({"result":result}), 200 
    
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f"An unexpected error occurred: {str(e)}"}), 500
    


# Add New Node API(from frontend get table_name,node_id,feedback_id,label,type)
@app.route('/add_node', methods=['POST'])
def add_node():
    data = request.json 
    try:
        result = sql_add_node(data=data)
        return result
    except Exception as e:
        return jsonify({"error": str(e)}), 500


#Add connection btn nodes(add connection in edges table)
@app.route('/add_connection', methods=['POST'])
def add_connection():
    data = request.json
    try:
        result = sql_add_connection(data=data)
        return result
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


#Merge Two node(from two table first take edge table and than node table)
@app.route('/merge_nodes', methods=['PUT'])
def merge_nodes():
    data = request.json
    try:
        # Execute edge updates first, then node updates
        edge_result = merge_edges(data)
        if isinstance(edge_result, tuple) and edge_result[1] == 500:
            return edge_result
            
        node_result = merge_nodes_data(data)
        if isinstance(node_result, tuple) and node_result[1] == 500:
            return node_result
            
        return jsonify({
            "edge_result": edge_result,
            "node_result": node_result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# DELETE API (delete row from node table and edge table)
@app.route('/delete', methods=['DELETE'])
def delete_record():
    data = request.json
    try:
        node_result = sql_delete_record_nodes(data)
        edge_result = sql_delete_record_edges(data)
        return jsonify({"delete_ndge_result":edge_result,
                        "detete_edge_result":node_result})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500




@app.route('/merge_multiple_nodes', methods=['PUT'])
def merge_multiple_nodes():
    data = request.json
    try:
        # body ={
        #     "table_name": "your_table",
        #     "nodes_to_merge": ["node1", "node2", "node3"],
        #     "new_label": "merged_node"
        # }
        # Validate input
        if not data.get('nodes_to_merge') or not data.get('new_label'):
            return jsonify({"error": "Missing nodes_to_merge or new_label"}), 400
        
        # Execute edge updates first, then node updates
        edge_result = merge_multiple_edges(data)
        if isinstance(edge_result, tuple) and edge_result[1] == 500:
            return edge_result
            
        node_result = merge_multiple_nodes_data(data)
        if isinstance(node_result, tuple) and node_result[1] == 500:
            return node_result
            
        return jsonify({
            "edge_result": edge_result,
            "node_result": node_result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500




# get data for Ranked Keywords Analysis by Geography and State-specific Keyword Analysis
@app.route('/get_node_rankings', methods=['GET'])
def get_node_rankings():
    try:
        # body = {
        #         "table_name":"2024 feedback",
        #         "metadata": metadata,
        #         "State":           column name
        #         "Stakeholder"
        #     }
        data = request.json
        data = get_node_by_geo(data)
        return jsonify({
            'data': data
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500


#Keywords by State
@app.route('/get_scatter_plot', methods=['GET'])
def get_scatter_plot():
    try:
        # body = {
        #         "table_name":"2024 feedback",
        #          "State":        column name
        #     }
        data = request.json
        data = get_scatter_data(data)
        return jsonify({
            'data': data
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

#Keywords by State
@app.route('/get_timeline_plot', methods=['GET'])
def get_timeline():
    try:
        # body = {
        #         "table_name":"2024 feedback",
        #         "data":                 columns name
        #     }
        data = request.json
        data = get_timeline_data(data)
        return jsonify({
            'data': data
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500


@app.route('/get_Stakeholder_data', methods=['GET'])
def get_Stakeholder_d():
    try:
        # body = {
        #         "table_name":"2024 feedback",
        #         "metadata": metadata,
        #          "state":,
        #          "Region":,
        #          "Stakeholder":
        #     }
        data = request.json
        data = get_stakeholder(data)
        return jsonify({
            'data': data
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/get_Treemap', methods=['GET'])
def get_Treemap_data():
    try:
        # body = {
        #         "table_name":"2024 feedback",
        #         "state":,
        #          "Region":,
        #          "Stakeholder":
        #     }
        data = request.json
        data = Tree_map(data)
        return jsonify({
            'data': data
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500
    

@app.route('/advance_search', methods=['GET'])
def search_feedback():
    data = request.get_json()
    # body = {
    #     "table_name":"demo table",
    #     "column_name":"Feedback/Quotes From Stakeholders",
    #     "search_text":"new",
    # }
    if not data:
        return jsonify({"error": "No search query provided"}), 400

    try:
        results = advance_search(data)
        return jsonify(results)

    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500   
    
if __name__ == '__main__':
    app.run(debug=True)
