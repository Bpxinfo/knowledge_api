import mysql.connector
import json
import time
from dateutil.parser import parse
import os
import re
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..')))
from src.exception import CustomException
from src.logger import logging
from typing import Dict, Any, Tuple
load_dotenv()

HOST= os.getenv('host')
# port = os.getenv('port')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')

def sanitize_sheetname(sheetname):
    """Sanitize the sheet name to make it a valid MySQL table name."""
    sanitized = re.sub(r'\W+', '_', sheetname)  # Replace non-word characters with underscores
    return sanitized.lower().strip('_') 

def create_connection():
    return mysql.connector.connect(
            host= HOST,
            user = USER,
            password = PASSWORD,
            database = DATABASE,
        )


def merge_edges(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update edges table to reflect node merger
    """
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        table_name = sanitize_sheetname(data['table_name'])
        edges_table = f"`{table_name}_edges`"
        first_node = data['first_node']
        second_node = data['second_node']
        new_label = data['new_label']
        
        # Update both source and target columns
        query = f"""
        UPDATE {edges_table}
        SET 
            source = CASE 
                WHEN source IN (%s, %s) THEN %s 
                ELSE source 
            END,
            target = CASE 
                WHEN target IN (%s, %s) THEN %s 
                ELSE target 
            END
        WHERE source IN (%s, %s) OR target IN (%s, %s)
        """
        values = (
            first_node, second_node, new_label,
            first_node, second_node, new_label,
            first_node, second_node, first_node, second_node
        )
        
        cursor.execute(query, values)
        connection.commit()
        
        return {
            "message": "Edges updated successfully",
            "rows_affected": cursor.rowcount
        }
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()

def merge_nodes_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update nodes table to merge two nodes into one
    """
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        table_name = sanitize_sheetname(data['table_name'])
        nodes_table = f"`{table_name}_nodes`"
        first_node = data['first_node']
        second_node = data['second_node']
        new_label = data['new_label']
        
        # Update the nodes
        query = f"""
        UPDATE {nodes_table}
        SET node_id = %s, label = %s
        WHERE node_id IN (%s, %s)
        """
        values = (new_label, new_label, first_node, second_node)
        
        cursor.execute(query, values)
        connection.commit()
        
        if cursor.rowcount > 0:
            return {
                "message": "Nodes merged successfully",
                "rows_affected": cursor.rowcount
            }
        return {"error": "No nodes found to merge"}, 404
        
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()


def sql_delete_record_nodes(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    table_name = data['table_name']
    table_name = sanitize_sheetname(sheetname=table_name)
    table_name = f"`{table_name}_nodes`"
    record_id = data['node_id']
    
    try:

        query = f"DELETE FROM {table_name} WHERE node_id = %s"
        cursor.execute(query, (record_id,))
        connection.commit()
        
        if cursor.rowcount > 0:
            return {"message": "Record deleted successfully"}, 200
        else:
            return {"error": "Record not found"}, 404
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()

def sql_delete_record_edges(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    table_name = data['table_name']
    table_name = sanitize_sheetname(sheetname=table_name)
    table_name = f"`{table_name}_edges`"
    source = data['node_id']
    target = data['node_id']

    try:

        query = f"DELETE FROM {table_name} WHERE source = %s AND target = %s"
        values = (source, target)
        cursor.execute(query, values)
        connection.commit()
        
        if cursor.rowcount > 0:
            return {"message": "Record deleted successfully"}, 200
        else:
            return {"error": "Record not found"}, 404
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()



def sql_add_node(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    #table name is node table
    table_name = data["table_name"]
    table_name = sanitize_sheetname(sheetname=table_name)
    table_name = f"`{table_name}_nodes`"
    try:
        # Insert query
        query = f"""
        INSERT INTO {table_name} (feedback_id, node_id, label, type)
        VALUES (%s, %s, %s, %s)
        """
        values = (
            data['feedback_id'], 
            data['node_id'], 
            data['label'], 
            data['type']
        )
        cursor.execute(query, values)
        connection.commit()

        return {
            "message": "Node added successfully",
            "new_node_id": cursor.lastrowid
        }, 201
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()


def sql_add_connection(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    # table is edge_table
    table_name = data["table_name"]
    table_name = sanitize_sheetname(sheetname=table_name)
    table_name = f"`{table_name}_edges`"
    try:
        # Insert query
        query = f"""
        INSERT INTO {table_name} (feedback_id, source,  relationship, target)
        VALUES (%s, %s, %s, %s)
        """
        values = (
            data['feedback_id'], 
            data['source'], 
            data['relationship'], 
            data['target']
        )
        cursor.execute(query, values)
        connection.commit()

        return {
            "message": "connection added successfully",
            "new_node_id": cursor.lastrowid
        }, 201
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()        



# Page Data editable

def merge_multiple_edges(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update edges table to reflect multiple node mergers
    """
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        table_name = sanitize_sheetname(data['table_name'])
        edges_table = f"`{table_name}_edges`"
        nodes_to_merge = data['nodes_to_merge']
        new_label = data['new_label']
        
        # Prepare the list of nodes to merge as a tuple for SQL query
        nodes_tuple = tuple(nodes_to_merge)
        
        # Update both source and target columns for multiple nodes
        query = f"""
        UPDATE {edges_table}
        SET 
            source = CASE 
                WHEN source IN {nodes_tuple} THEN %s 
                ELSE source 
            END,
            target = CASE 
                WHEN target IN {nodes_tuple} THEN %s 
                ELSE target 
            END
        WHERE source IN {nodes_tuple} OR target IN {nodes_tuple}
        """
        
        cursor.execute(query, (new_label, new_label))
        connection.commit()
        
        return {
            "message": "Edges updated successfully",
            "rows_affected": cursor.rowcount
        }
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()

def merge_multiple_nodes_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update nodes table to merge multiple nodes into one
    """
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        table_name = sanitize_sheetname(data['table_name'])
        nodes_table = f"`{table_name}_nodes`"
        nodes_to_merge = data['nodes_to_merge']
        new_label = data['new_label']
        
        # Prepare the list of nodes to merge as a tuple for SQL query
        nodes_tuple = tuple(nodes_to_merge)
        
        # Update the nodes
        query = f"""
        UPDATE {nodes_table}
        SET node_id = %s, label = %s
        WHERE node_id IN {nodes_tuple}
        """
        
        cursor.execute(query, (new_label, new_label))
        connection.commit()
        
        if cursor.rowcount > 0:
            return {
                "message": "Nodes merged successfully",
                "rows_affected": cursor.rowcount
            }
        return {"error": "No nodes found to merge"}, 404
        
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        connection.close()




# Page 3 

# def get_node_by_geo():
#     # Create database connection
#         connection = create_connection()
#         cursor = connection.cursor(dictionary=True)
        
#         # Get table names from request or use defaults
#         nodes_table = "`2024_feedback_nodes`"
#         metadata_table = "`2024_feedback`"
#         edges_table ="`2024_feedback_edges`"
        
#         # Fetch unique nodes
#         cursor.execute(f"""
#             SELECT DISTINCT id, label, type 
#             FROM {nodes_table}
#         """)
#         unique_nodes = cursor.fetchall()
        
#         ranked_data = []
        
#         # Process each unique node
#         for node in unique_nodes:
#             node_id = node['id']
            
#             # Get document indices for this node
#             cursor.execute(f"""
#                 SELECT DISTINCT id 
#                 FROM {nodes_table} 
#                 WHERE id = %s
#             """, (node_id,))
#             doc_indices = [row['id'] for row in cursor.fetchall()]
            
#             # Get related metadata
#             cursor.execute(f"""
#                 SELECT DISTINCT State, stakeholder, Date, `Feedback/Quotes From Stakeholders` 
#                 FROM {metadata_table} 
#                 WHERE id IN ({','.join(map(str, doc_indices))})
#             """)
#             related_meta = cursor.fetchall()
            
#             # Get related edges
#             cursor.execute(f"""
#                 SELECT * 
#                 FROM {edges_table} 
#                 WHERE source = %s OR target = %s
#             """, (node_id, node_id))
#             related_edges = cursor.fetchall()
            
#             # Prepare unique geographies and stakeholders
#             unique_geographies = list(set(meta['State'] for meta in related_meta if meta['State']))
#             unique_stakeholders = list(set(meta['stakeholder'] for meta in related_meta if meta['stakeholder']))
            
#             # Prepare sample references
#             sample_references = list(set(meta['Feedback/Quotes From Stakeholders'] for meta in related_meta))[:2]
            
#             # Get date range
#             dates = [meta['Date'] for meta in related_meta if meta['Date']]
            
#             ranked_data.append({
#                 'Keyword': node['label'],
#                 'Total Mentions': len(doc_indices),
#                 'Type': node['type'],
#                 'Geographies': ', '.join(unique_geographies),
#                 'Stakeholders': ', '.join(unique_stakeholders),
#                 'First Mention': min(dates) if dates else None,
#                 'Last Mention': max(dates) if dates else None,
#                 'Sample References': ', '.join(sample_references)
#             })
        
#         # Close database connection
#         cursor.close()
#         connection.close()
        
        
#         return ranked_data   




def get_node_by_geo(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    node_table = f"`{santiize_name}_nodes`"
    print(node_table)
    column_name = "`Feedback/Quotes From Stakeholders`"
    # column_name = data['meta_data']
    try:
        query = f"""
                WITH feedback_nodes AS (
                    SELECT 
                        n.node_id AS node_id,
                        n.label AS keyword,
                        COUNT(DISTINCT m.id) AS total_mentions,
                        GROUP_CONCAT(DISTINCT m.{column_name} SEPARATOR '; ') AS sample_references,
                        GROUP_CONCAT(DISTINCT m.State SEPARATOR ', ') AS states,
                        GROUP_CONCAT(DISTINCT m.Stakeholder SEPARATOR ', ') AS stakeholders
                    FROM 
                        {node_table} n
                    LEFT JOIN 
                        {table_name} m ON n.feedback_id = m.id
                    GROUP BY 
                        n.node_id, n.label
                )
                SELECT 
                    node_id,
                    keyword,
                    total_mentions,
                    SUBSTRING_INDEX(sample_references, ';', 2) AS sample_references,
                    states,
                    stakeholders
                FROM 
                    feedback_nodes
                ORDER BY 
                    total_mentions DESC;
                """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        return results
    
    except Exception as e:
        # Close cursor and connection in case of error
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        
        return {"error": str(e)}, 500


def get_scatter_data(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    node_table = f"`{santiize_name}_nodes`"
 
    # column_name = data['meta_data']
    try:
        query = f"""
                SELECT 
                    m.state, 
                    n.label AS keyword, 
                    n.type, 
                    COUNT(n.label) AS label_count
                FROM 
                    {node_table} n
                JOIN 
                    {table_name} m ON n.feedback_id = m.id
                GROUP BY 
                    m.state, n.label, n.type
                ORDER BY 
                    label_count DESC;

                """  
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        
        return results
    
    except Exception as e:
        # Close cursor and connection in case of error
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        
        return {"error": str(e)}, 500