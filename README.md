# Teaching Organization Analytics

A comprehensive analytics application for teaching organizations to analyze client buying trends, program popularity, opportunity pipelines, sales forecasts, and program profitability.

## Features

- **Drag-and-Drop Data Import**: Upload your data in CSV, Excel, or JSON formats
- **Client Trends Analysis**: Analyze buying patterns by client type, industry, and region
- **Program Popularity Tracking**: Identify your most successful programs
- **Opportunity Pipeline Monitoring**: Track sales pipeline and conversion rates
- **Profitability Tracking**: Calculate program profitability (revenue - costs)
- **Custom Analysis**: Run analyses using natural language prompts
- **Interactive Visualizations**: View data through charts, reports, and tables

## Getting Started

1. Access the application at: [Teaching Analytics App](https://teaching-analytics.streamlit.app)
2. Import your data using the drag-and-drop interface
3. Navigate through the different analysis modules using the sidebar

## Data Requirements

The application requires data in the following formats:

### Clients
CSV/Excel with columns: client_id, name, industry, size, region, contact_person, email, phone, first_engagement_date

### Programs
CSV/Excel with columns: program_id, name, category, delivery_mode, duration_days, price_per_participant

### Enrollments
CSV/Excel with columns: enrollment_id, client_id, program_id, start_date, participants, revenue, trainer_cost, logistics_cost, venue_cost, utilities_cost

### Opportunities
CSV/Excel with columns: opportunity_id, client_id, program_id, stage, expected_value, probability, expected_close_date

## License

This project is licensed under the MIT License - see the LICENSE file for details.
