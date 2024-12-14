import mysql.connector
import json
import time
from dateutil.parser import parse
import os
import re
import pandas as pd
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..')))
from src.utils import sanitize_sheetname
from src.exception import CustomException
from src.logger import logging
from typing import Dict, Any, Tuple
load_dotenv()

HOST= os.getenv('host')
# port = os.getenv('port')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


def create_connection():
    return mysql.connector.connect(
            host= HOST,
            user = USER,
            password = PASSWORD,
            database = DATABASE,
        )


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
        
        logging.info("error ", str(e))
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
        logging.info("error ", str(e))
        return {"error": str(e)}, 500
    

def get_timeline_data(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    node_table = f"`{santiize_name}_nodes`"

    try:
        query = f"""
                SELECT 
                    m.date,
                    n.node_id,
                    COUNT(n.node_id) AS frequency
                FROM 
                    {node_table} n
                JOIN 
                    {table_name} m ON n.feedback_id = m.id
                GROUP BY 
                    m.date, n.node_id
                ORDER BY 
                    n.node_id
                LIMIT 0, 1000;

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
        logging.info("error ", str(e))
        return {"error": str(e)}, 500


def get_stakeholder(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    metadata = data['meta_data']
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    node_table = f"`{santiize_name}_nodes`"
    
    try:
        query = f"""
                SELECT 
                    m.Region, 
                    m.State, 
                    m.Stakeholder, 
                    n.label AS Keyword, 
                    m.`{metadata}` AS ref,
                    COUNT(m.Stakeholder) AS Count
                FROM 
                    {table_name} m
                JOIN 
                    {node_table} n ON m.id = n.feedback_id
                GROUP BY 
                    m.Region, m.State, m.Stakeholder, n.label,m.`{metadata}`
                ORDER BY 
                    m.Region, m.State, m.Stakeholder, n.label,m.`{metadata}`;
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
        logging.info("error ", str(e))
        return {"error": str(e)}, 500


def Tree_map(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    metadata = data['meta_data']
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    node_table = f"`{santiize_name}_nodes`"
    
    try:
        query = f"""
               SELECT 
                    m.State, 
                    m.Region, 
                    m.Stakeholder, 
                    n.node_id, 
                    n.type,
                    COUNT(*) AS record_count
                FROM 
                    {table_name}  m
                JOIN 
                    {node_table} n ON m.id = n.feedback_id
                GROUP BY 
                    m.State, 
                    m.Region, 
                    m.Stakeholder, 
                    n.node_id, 
                    n.type
                ORDER BY 
                    record_count DESC;
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
        logging.info("error ", str(e))
        return {"error": str(e)}, 500
    

def advance_search(data):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    column_name = data["column_name"]
    column_name = f"`{column_name}`"
    search_text = data["search_text"]
    try:
        query = f"""
                SELECT * 
                FROM {table_name}
                WHERE {column_name} LIKE %s;
                """  
        cursor.execute(query, (f"%{search_text}%",))
        results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return results
    
    except Exception as e:
        logging.info("error ", str(e))
        return {"error": str(e)}    


def get_treatment(data):
    connection = create_connection()
    # cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    try:
        query = f"""
        SELECT Treatment, State, Stakeholder, Date 
        FROM `{table_name}`;
        """
        df = pd.read_sql(query, connection)

        # Close the connection
        df['Treatment'] = df['Treatment'].fillna('').str.split(',')

        # Explode the Treatment list into multiple rows
        df_exploded = df.explode('Treatment').query("Treatment != ''")

        # Create Pivot Table
        State_heatmap_data = df_exploded.pivot_table(
            index='State', columns='Treatment', values='Stakeholder', aggfunc='count', fill_value=0
        )
        
        # region_heatmap_data = df_exploded.pivot_table(
        #     index='Region', columns='Treatment', values='Stakeholder', aggfunc='count', fill_value=0
        # )

         # Convert to JSON
        State_heatmap_data = State_heatmap_data.to_dict(orient='records')
        # region_heatmap_data = region_heatmap_data.to_dict(orient='records')
        return State_heatmap_data

    except Exception as e:
        logging.info("error ", str(e))
        return {"error": str(e)}
    
def get_safety(data):
    connection = create_connection()
    # cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    try:
        query = f"""
        SELECT Safety, State, Stakeholder, Date 
        FROM `{table_name}`;
        """
        df = pd.read_sql(query, connection)

        # Close the connection
        df['Safety'] = df['Safety'].fillna('').str.split(',')

        # Explode the Treatment list into multiple rows
        df_exploded = df.explode('Safety').query("Safety != ''")

        # Create Pivot Table
        State_heatmap_data = df_exploded.pivot_table(
            index='State', columns='Safety', values='Stakeholder', aggfunc='count', fill_value=0
        )
        
        # region_heatmap_data = df_exploded.pivot_table(
        #     index='Region', columns='Safety', values='Stakeholder', aggfunc='count', fill_value=0
        # )

         # Convert to JSON
        State_heatmap_data = State_heatmap_data.to_dict(orient='records')
        # region_heatmap_data = region_heatmap_data.to_dict(orient='records')
        return State_heatmap_data

    except Exception as e:
        logging.info("error ", str(e))
        return {"error": str(e)}

def get_diagnosis(data):
    connection = create_connection()
    # cursor = connection.cursor(dictionary=True)
    santiize_name = sanitize_sheetname(data['table_name'])
    table_name = f"`{santiize_name}`"
    try:
        query = f"""
        SELECT Diagnosis, State, Stakeholder, Date 
        FROM `{table_name}`;
        """
        df = pd.read_sql(query, connection)

        # Close the connection
        df['Diagnosis'] = df['Diagnosis'].fillna('').str.split(',')

        # Explode the Treatment list into multiple rows
        df_exploded = df.explode('Diagnosis').query("Diagnosis != ''")

        # Create Pivot Table
        State_heatmap_data = df_exploded.pivot_table(
            index='State', columns='Diagnosis', values='Stakeholder', aggfunc='count', fill_value=0
        )
        
        # region_heatmap_data = df_exploded.pivot_table(
        #     index='Region', columns='Diagnosis', values='Stakeholder', aggfunc='count', fill_value=0
        # )

         # Convert to JSON
        State_heatmap_data = State_heatmap_data.to_dict(orient='records')
        # region_heatmap_data = region_heatmap_data.to_dict(orient='records')
        return State_heatmap_data

    except Exception as e:
        logging.info("error ", str(e))
        return {"error": str(e)}    