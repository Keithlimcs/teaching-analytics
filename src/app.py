import streamlit as st
import pandas as pd
import sqlite3
import os
import io
import json
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Teaching Organization Analytics - Data Import",
    page_icon="📊",
    layout="wide"
)

# Create database directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Database connection
conn = sqlite3.connect('data/teaching_analytics.db')
cursor = conn.cursor()

# Function to create database schema
def create_database_schema():
    with open('docs/database_schema.sql', 'r') as f:
        schema_script = f.read()
    
    # Split the script by semicolons to execute each statement separately
    statements = schema_script.split(';')
    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
            except sqlite3.Error as e:
                st.error(f"Error executing SQL statement: {e}")
    
    conn.commit()

# Function to check if tables exist
def check_tables_exist():
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clients'")
    return cursor.fetchone() is not None

# Create schema if tables don't exist
if not check_tables_exist():
    create_database_schema()
    st.success("Database schema created successfully!")

# Main title
st.title("Teaching Organization Analytics")
st.subheader("Data Import Interface")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Data Import", "Client Analysis", "Program Analysis", 
                                 "Opportunity Pipeline", "Profitability Tracking", 
                                 "Custom Analysis"])

# Data Import Page
if page == "Data Import":
    st.header("Import Your Data")
    st.write("""
    Upload your data files by dragging and dropping them into the appropriate sections below.
    Supported formats: CSV, Excel, and JSON.
    """)
    
    # Create tabs for different data types
    tab1, tab2, tab3, tab4 = st.tabs(["Clients", "Programs", "Enrollments", "Opportunities"])
    
    with tab1:
        st.subheader("Import Client Data")
        
        # Display expected format
        with st.expander("Expected Format"):
            st.write("""
            Your client data should include the following columns:
            - client_id (optional, will be auto-generated if not provided)
            - name (required)
            - industry
            - size (Small, Medium, Large, Enterprise)
            - region
            - contact_person
            - email
            - phone
            - first_engagement_date (YYYY-MM-DD)
            - last_engagement_date (YYYY-MM-DD)
            - total_spend
            - notes
            """)
            
            # Example data
            st.code("""
            Example CSV:
            name,industry,size,region,contact_person,email
            Acme Corp,Technology,Large,North,John Smith,john@acme.com
            Beta Inc,Healthcare,Medium,South,Jane Doe,jane@beta.com
            """)
        
        # File uploader
        client_file = st.file_uploader("Upload Client Data", type=["csv", "xlsx", "json"], key="client_upload")
        
        if client_file is not None:
            try:
                # Determine file type and read accordingly
                file_extension = client_file.name.split(".")[-1].lower()
                
                if file_extension == "csv":
                    df = pd.read_csv(client_file)
                elif file_extension == "xlsx":
                    df = pd.read_excel(client_file)
                elif file_extension == "json":
                    df = pd.read_json(client_file)
                
                # Display preview
                st.write("Data Preview:")
                st.dataframe(df.head())
                
                # Validation
                required_columns = ["name"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"Missing required columns: {', '.join(missing_columns)}")
                else:
                    # Import button
                    if st.button("Import Client Data"):
                        # Prepare data for import
                        if "client_id" not in df.columns:
                            # Get the next available ID
                            cursor.execute("SELECT MAX(client_id) FROM clients")
                            max_id = cursor.fetchone()[0]
                            start_id = 1 if max_id is None else max_id + 1
                            df["client_id"] = range(start_id, start_id + len(df))
                        
                        # Convert DataFrame to SQL
                        df.to_sql("clients", conn, if_exists="append", index=False)
                        
                        st.success(f"Successfully imported {len(df)} client records!")
                        
                        # Display current data count
                        cursor.execute("SELECT COUNT(*) FROM clients")
                        count = cursor.fetchone()[0]
                        st.info(f"Total clients in database: {count}")
            
            except Exception as e:
                st.error(f"Error importing data: {str(e)}")
    
    with tab2:
        st.subheader("Import Program Data")
        
        # Display expected format
        with st.expander("Expected Format"):
            st.write("""
            Your program data should include the following columns:
            - program_id (optional, will be auto-generated if not provided)
            - name (required)
            - description
            - category (e.g., Leadership, Technical, Soft Skills)
            - delivery_mode (In-Person, Virtual, Hybrid)
            - duration (hours)
            - base_price
            - min_participants
            - max_participants
            - trainer_cost_per_session
            - materials_cost_per_participant
            - active (0 or 1)
            - creation_date (YYYY-MM-DD)
            - last_updated (YYYY-MM-DD)
            """)
            
            # Example data
            st.code("""
            Example CSV:
            name,category,delivery_mode,duration,base_price
            Leadership Essentials,Leadership,In-Person,16,1200
            Python Programming,Technical,Virtual,24,1500
            """)
        
        # File uploader
        program_file = st.file_uploader("Upload Program Data", type=["csv", "xlsx", "json"], key="program_upload")
        
        if program_file is not None:
            try:
                # Determine file type and read accordingly
                file_extension = program_file.name.split(".")[-1].lower()
                
                if file_extension == "csv":
                    df = pd.read_csv(program_file)
                elif file_extension == "xlsx":
                    df = pd.read_excel(program_file)
                elif file_extension == "json":
                    df = pd.read_json(program_file)
                
                # Display preview
                st.write("Data Preview:")
                st.dataframe(df.head())
                
                # Validation
                required_columns = ["name"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"Missing required columns: {', '.join(missing_columns)}")
                else:
                    # Import button
                    if st.button("Import Program Data"):
                        # Prepare data for import
                        if "program_id" not in df.columns:
                            # Get the next available ID
                            cursor.execute("SELECT MAX(program_id) FROM programs")
                            max_id = cursor.fetchone()[0]
                            start_id = 1 if max_id is None else max_id + 1
                            df["program_id"] = range(start_id, start_id + len(df))
                        
                        # Convert DataFrame to SQL
                        df.to_sql("programs", conn, if_exists="append", index=False)
                        
                        st.success(f"Successfully imported {len(df)} program records!")
                        
                        # Display current data count
                        cursor.execute("SELECT COUNT(*) FROM programs")
                        count = cursor.fetchone()[0]
                        st.info(f"Total programs in database: {count}")
            
            except Exception as e:
                st.error(f"Error importing data: {str(e)}")
    
    with tab3:
        st.subheader("Import Enrollment Data")
        
        # Display expected format
        with st.expander("Expected Format"):
            st.write("""
            Your enrollment data should include the following columns:
            - enrollment_id (optional, will be auto-generated if not provided)
            - program_id (required, must exist in programs table)
            - client_id (required, must exist in clients table)
            - start_date (YYYY-MM-DD)
            - end_date (YYYY-MM-DD)
            - location
            - delivery_mode (In-Person, Virtual, Hybrid)
            - num_participants
            - revenue
            - trainer_cost
            - logistics_cost
            - venue_cost
            - utilities_cost
            - materials_cost
            - status (Scheduled, Completed, Cancelled)
            - feedback_score
            - notes
            """)
            
            # Example data
            st.code("""
            Example CSV:
            program_id,client_id,start_date,end_date,num_participants,revenue,trainer_cost,status
            1,1,2024-01-15,2024-01-16,12,14400,2000,Completed
            2,2,2024-02-10,2024-02-12,8,12000,1800,Completed
            """)
        
        # File uploader
        enrollment_file = st.file_uploader("Upload Enrollment Data", type=["csv", "xlsx", "json"], key="enrollment_upload")
        
        if enrollment_file is not None:
            try:
                # Determine file type and read accordingly
                file_extension = enrollment_file.name.split(".")[-1].lower()
                
                if file_extension == "csv":
                    df = pd.read_csv(enrollment_file)
                elif file_extension == "xlsx":
                    df = pd.read_excel(enrollment_file)
                elif file_extension == "json":
                    df = pd.read_json(enrollment_file)
                
                # Display preview
                st.write("Data Preview:")
                st.dataframe(df.head())
                
                # Validation
                required_columns = ["program_id", "client_id"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"Missing required columns: {', '.join(missing_columns)}")
                else:
                    # Import button
                    if st.button("Import Enrollment Data"):
                        # Prepare data for import
                        if "enrollment_id" not in df.columns:
                            # Get the next available ID
                            cursor.execute("SELECT MAX(enrollment_id) FROM enrollments")
                            max_id = cursor.fetchone()[0]
                            start_id = 1 if max_id is None else max_id + 1
                            df["enrollment_id"] = range(start_id, start_id + len(df))
                        
                        # Convert DataFrame to SQL
                        df.to_sql("enrollments", conn, if_exists="append", index=False)
                        
                        st.success(f"Successfully imported {len(df)} enrollment records!")
                        
                        # Display current data count
                        cursor.execute("SELECT COUNT(*) FROM enrollments")
                        count = cursor.fetchone()[0]
                        st.info(f"Total enrollments in database: {count}")
            
            except Exception as e:
                st.error(f"Error importing data: {str(e)}")
    
    with tab4:
        st.subheader("Import Opportunity Data")
        
        # Display expected format
        with st.expander("Expected Format"):
            st.write("""
            Your opportunity data should include the following columns:
            - opportunity_id (optional, will be auto-generated if not provided)
            - client_id (required, must exist in clients table)
            - program_id (required, must exist in programs table)
            - potential_revenue
            - estimated_participants
            - stage (Lead, Prospect, Proposal, Negotiation, Closed Won, Closed Lost)
            - probability (0-100%)
            - expected_close_date (YYYY-MM-DD)
            - actual_close_date (YYYY-MM-DD)
            - created_date (YYYY-MM-DD)
            - last_updated (YYYY-MM-DD)
            - owner
            - notes
            """)
            
            # Example data
            st.code("""
            Example CSV:
            client_id,program_id,potential_revenue,stage,probability,expected_close_date
            3,1,18000,Proposal,60,2024-05-15
            4,2,24000,Negotiation,80,2024-04-30
            """)
        
        # File uploader
        opportunity_file = st.file_uploader("Upload Opportunity Data", type=["csv", "xlsx", "json"], key="opportunity_upload")
        
        if opportunity_file is not None:
            try:
                # Determine file type and read accordingly
                file_extension = opportunity_file.name.split(".")[-1].lower()
                
                if file_extension == "csv":
                    df = pd.read_csv(opportunity_file)
                elif file_extension == "xlsx":
                    df = pd.read_excel(opportunity_file)
                elif file_extension == "json":
                    df = pd.read_json(opportunity_file)
                
                # Display preview
                st.write("Data Preview:")
                st.dataframe(df.head())
                
                # Validation
                required_columns = ["client_id", "program_id"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"Missing required columns: {', '.join(missing_columns)}")
                else:
                    # Import button
                    if st.button("Import Opportunity Data"):
                        # Prepare data for import
                        if "opportunity_id" not in df.columns:
                            # Get the next available ID
                            cursor.execute("SELECT MAX(opportunity_id) FROM opportunities")
                            max_id = cursor.fetchone()[0]
                            start_id = 1 if max_id is None else max_id + 1
                            df["opportunity_id"] = range(start_id, start_id + len(df))
                        
                        # Convert DataFrame to SQL
                        df.to_sql("opportunities", conn, if_exists="append", index=False)
                        
                        st.success(f"Successfully imported {len(df)} opportunity records!")
                        
                        # Display current data count
                        cursor.execute("SELECT COUNT(*) FROM opportunities")
                        count = cursor.fetchone()[0]
                        st.info(f"Total opportunities in database: {count}")
            
            except Exception as e:
                st.error(f"Error importing data: {str(e)}")
    
    # Data export section
    st.header("Export Data")
    st.write("Export your current database tables to CSV files.")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("Export Clients"):
            clients_df = pd.read_sql("SELECT * FROM clients", conn)
            csv = clients_df.to_csv(index=False)
            st.download_button(
                label="Download Clients CSV",
                data=csv,
                file_name="clients_export.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("Export Programs"):
            programs_df = pd.read_sql("SELECT * FROM programs", conn)
            csv = programs_df.to_csv(index=False)
            st.download_button(
                label="Download Programs CSV",
                data=csv,
                file_name="programs_export.csv",
                mime="text/csv"
            )
    
    with col3:
        if st.button("Export Enrollments"):
            enrollments_df = pd.read_sql("SELECT * FROM enrollments", conn)
            csv = enrollments_df.to_csv(index=False)
            st.download_button(
                label="Download Enrollments CSV",
                data=csv,
                file_name="enrollments_export.csv",
                mime="text/csv"
            )
    
    with col4:
        if st.button("Export Opportunities"):
            opportunities_df = pd.read_sql("SELECT * FROM opportunities", conn)
            csv = opportunities_df.to_csv(index=False)
            st.download_button(
                label="Download Opportunities CSV",
                data=csv,
                file_name="opportunities_export.csv",
                mime="text/csv"
            )
    
    # Database statistics
    st.header("Database Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    cursor.execute("SELECT COUNT(*) FROM clients")
    client_count = cursor.fetchone()[0]
    col1.metric("Clients", client_count)
    
    cursor.execute("SELECT COUNT(*) FROM programs")
    program_count = cursor.fetchone()[0]
    col2.metric("Programs", program_count)
    
    cursor.execute("SELECT COUNT(*) FROM enrollments")
    enrollment_count = cursor.fetchone()[0]
    col3.metric("Enrollments", enrollment_count)
    
    cursor.execute("SELECT COUNT(*) FROM opportunities")
    opportunity_count = cursor.fetchone()[0]
    col4.metric("Opportunities", opportunity_count)

# Placeholder for other pages
elif page == "Client Analysis":
    st.header("Client Trends Analysis")
    st.info("This section will be implemented in the next phase.")
    
elif page == "Program Analysis":
    st.header("Program Popularity Tracking")
    st.info("This section will be implemented in the next phase.")
    
elif page == "Opportunity Pipeline":
    st.header("Opportunity Pipeline & Forecasting")
    st.info("This section will be implemented in the next phase.")
    
elif page == "Profitability Tracking":
    st.header("Profitability Tracking")
    st.info("This section will be implemented in the next phase.")
    
elif page == "Custom Analysis":
    st.header("Custom Analysis")
    st.info("This section will be implemented in the next phase.")

# Close connection when app is done
conn.close()
