from flask import Flask, request, json
from flask_cors import CORS
import os
import psycopg2
import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
import pycountry
import joblib

# Initialize the Flask application
app = Flask(__name__)
CORS(app)
# CORS(app, resources={r"/*": {"origins": [
#     "https://datascience-salary-vb.web.app",
#     "http://localhost:3000" 
# ]}})

# Database configuration
db_host = os.getenv("DB_HOST", "db")
db_name = os.getenv("DB_NAME", "salary_db")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "admin")

# Load the pre-trained model
MODEL_PATH = './main_models/LinearRegression()_model_final.pkl'  # Ensure the model file is available in this location
with open(MODEL_PATH, 'rb') as file:
    model = joblib.load(file)


from sklearn.preprocessing import LabelEncoder

def iso2_to_iso3(iso2_code):
    try:
        return pycountry.countries.get(alpha_2=iso2_code).alpha_3
    except:
        return None

def preprocess_json_data(json_data):
    df = pd.DataFrame([json_data])
    df['employee_residence_iso3'] = df['employee_residence'].apply(iso2_to_iso3)
    experience_mapping = {'EN': 'Entry', 'MI': 'Mid', 'SE': 'Senior', 'EX': 'Executive'}
    df['experience_level'] = df['experience_level'].replace(experience_mapping)
    employment_mapping = {'Full-time': 'FT', 'Part-time': 'PT', 'Freelance': 'FL', 'Contract': 'CT'}
    df['employment_type'] = df['employment_type'].replace(employment_mapping)
    df['experience_level'] = df['experience_level'].str.upper()
    df['employment_type'] = df['employment_type'].str.upper()
    df['company_size'] = df['company_size'].str.upper()
    df['remote_ratio'] = pd.to_numeric(df['remote_ratio'], errors='coerce')
    df['remote_ratio'] = df['remote_ratio'].fillna(0)
    df['remote_category'] = pd.cut(df['remote_ratio'], 
                                       bins=[-1, 0, 50, 100], 
                                       labels=['Onsite', 'Hybrid', 'Remote'])
    region_mapping = {
    'US': 'North America',
    'CA': 'North America',
    'GB': 'Europe',
    'FR': 'Europe',
    'DE': 'Europe',
    'IN': 'Asia-Pacific',
    'CN': 'Asia-Pacific',
    'ES': 'Europe',
    'IT': 'Europe',
    'AU': 'Oceania',
    'BR': 'South America',
    }
    encoders = joblib.load('main_models/label_encoders_with_mappings.pkl')
    df['company_region'] = df['company_location'].map(region_mapping).fillna('Other')
    print(encoders)
    for column in ['experience_level', 'employment_type', 'company_size', 'company_location']:
        if column in encoders:
            mapping = encoders[column]["classes"]
            df[column] = df[column].apply(
                lambda x: mapping[x] if x in mapping else -1 
            )
    return df



# Connect to the PostgreSQL database
def get_db_connection():
    connection = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS new_predictions (
            work_year INTEGER,
            experience_level VARCHAR(100),
            employment_type VARCHAR(100),
            job_title VARCHAR(100),
            employee_residence VARCHAR(100),
            remote_ratio INTEGER,
            company_location VARCHAR(100),
            company_size VARCHAR(100),
            predicted_salary FLOAT
        )
    ''')
    connection.commit()
    print("Database Connected")
    return connection

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json
    
    # Validate input data
    required_fields = [
        'work_year', 'experience_level', 'employment_type', 'job_title', 
        'employee_residence', 'remote_ratio', 'company_location', 'company_size'
    ]
    for field in required_fields:
        if field not in data:
            return json.jsonify({"error": f"Missing field: {field}"}), 400
    
    processed_data = preprocess_json_data(data)
    model = joblib.load('main_models/LinearRegression()_model_final.pkl')
    predicted_salary = model.predict(processed_data)
    predicted_salary_value = float(predicted_salary[0])

    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT INTO new_predictions (
            work_year, experience_level, employment_type, job_title,
            employee_residence, remote_ratio, company_location,
            company_size, predicted_salary
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''',
        (
            data['work_year'], data['experience_level'], data['employment_type'], 
            data['job_title'], data['employee_residence'], data['remote_ratio'], 
            data['company_location'], data['company_size'], predicted_salary_value
        )
    )
    connection.commit()
    cursor.close()
    connection.close()
    
    return json.jsonify({"predicted_salary": predicted_salary_value})

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)

