# Teaching Organization Analytics - Deployment Guide

This guide provides instructions for deploying the Teaching Organization Analytics application with drag-and-drop data import functionality.

## Application Overview

The Teaching Organization Analytics application helps teaching organizations analyze:
- Client buying trends
- Program popularity
- Opportunity pipelines
- Sales forecasts
- Program profitability (revenue - costs)

All data is provided by users through an intuitive drag-and-drop interface, ensuring complete data privacy and control.

## Deployment Options

### Option 1: Streamlit Cloud (Recommended)

1. **Create a GitHub repository**
   - Sign up/in to GitHub (https://github.com)
   - Create a new repository (e.g., "teaching-analytics")
   - Clone the repository to your local machine

2. **Upload the application files**
   - Copy all files from this deployment package to your local repository
   - Commit and push the changes to GitHub

3. **Deploy on Streamlit Cloud**
   - Go to https://streamlit.io/cloud
   - Sign in with your GitHub account
   - Click "New app"
   - Select your repository, branch (main), and the main file path (app.py)
   - Click "Deploy"

Your application will be available at: `https://[your-username]-teaching-analytics.streamlit.app`

### Option 2: Heroku

1. **Create a Heroku account** at https://heroku.com
2. **Install Heroku CLI** from https://devcenter.heroku.com/articles/heroku-cli
3. **Login and create app**:
   ```
   heroku login
   heroku create teaching-analytics
   ```
4. **Deploy the application**:
   ```
   git init
   git add .
   git commit -m "Initial commit"
   git push heroku main
   ```

### Option 3: Local Deployment

1. **Install dependencies**:
   ```
   pip install -r requirements.txt
   ```
2. **Run the application**:
   ```
   streamlit run app.py
   ```

## Data Format Requirements

The application expects data in the following formats:

### Clients
CSV/Excel with columns: client_id, name, industry, size, region, contact_person, email, phone, first_engagement_date

### Programs
CSV/Excel with columns: program_id, name, category, delivery_mode, duration_days, price_per_participant

### Enrollments
CSV/Excel with columns: enrollment_id, client_id, program_id, start_date, participants, revenue, trainer_cost, logistics_cost, venue_cost, utilities_cost

### Opportunities
CSV/Excel with columns: opportunity_id, client_id, program_id, stage, expected_value, probability, expected_close_date

## Customization

You can customize the application by modifying:
- `app.py` - Main application file
- `.streamlit/config.toml` - Streamlit configuration (theme, behavior)

## Support

For questions or support, please contact the development team.
