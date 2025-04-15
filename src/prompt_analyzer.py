import pandas as pd
import numpy as np
import sqlite3
import re
import string
from collections import Counter
import streamlit as st

class PromptAnalyzer:
    """
    A class to analyze natural language prompts and generate appropriate analytics.
    Provides a prompt-based interface for custom analysis in the Teaching Organization Analytics application.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the analyzer with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # Define keywords for different analysis types
        self.keywords = {
            'entity_types': {
                'client': ['client', 'clients', 'customer', 'customers', 'organization', 'organizations'],
                'program': ['program', 'programs', 'course', 'courses', 'training', 'trainings'],
                'enrollment': ['enrollment', 'enrollments', 'registration', 'registrations'],
                'opportunity': ['opportunity', 'opportunities', 'pipeline', 'deal', 'deals', 'lead', 'leads']
            },
            'metrics': {
                'revenue': ['revenue', 'sales', 'income', 'earnings', 'money', 'payment', 'payments'],
                'profit': ['profit', 'profits', 'margin', 'margins', 'profitability', 'earnings'],
                'cost': ['cost', 'costs', 'expense', 'expenses', 'spending', 'expenditure'],
                'count': ['count', 'number', 'quantity', 'total', 'amount'],
                'trend': ['trend', 'trends', 'over time', 'history', 'historical', 'pattern', 'patterns']
            },
            'dimensions': {
                'industry': ['industry', 'industries', 'sector', 'sectors'],
                'region': ['region', 'regions', 'location', 'locations', 'area', 'areas', 'geography'],
                'size': ['size', 'sizes', 'company size', 'organization size'],
                'category': ['category', 'categories', 'type', 'types'],
                'delivery_mode': ['delivery mode', 'delivery', 'mode', 'online', 'in-person', 'virtual', 'classroom'],
                'time': ['time', 'month', 'months', 'year', 'years', 'quarter', 'quarters', 'date', 'dates', 'period']
            },
            'query_types': {
                'top': ['top', 'best', 'highest', 'most', 'largest', 'biggest', 'greatest'],
                'bottom': ['bottom', 'worst', 'lowest', 'least', 'smallest'],
                'average': ['average', 'avg', 'mean', 'median', 'typical'],
                'comparison': ['compare', 'comparison', 'versus', 'vs', 'against', 'difference', 'differences'],
                'distribution': ['distribution', 'breakdown', 'composition', 'makeup', 'split', 'segmentation']
            },
            'time_periods': {
                'this_month': ['this month', 'current month'],
                'last_month': ['last month', 'previous month'],
                'this_quarter': ['this quarter', 'current quarter'],
                'last_quarter': ['last quarter', 'previous quarter'],
                'this_year': ['this year', 'current year'],
                'last_year': ['last year', 'previous year']
            }
        }
        
        # Define common analysis templates
        self.templates = {
            'top_clients_by_revenue': {
                'query_type': 'top',
                'entity_type': 'clients',
                'metric': 'revenue',
                'dimension': 'client_name',
                'title': 'Top Clients by Revenue',
                'description': 'Shows the clients that have generated the most revenue'
            },
            'top_programs_by_revenue': {
                'query_type': 'top',
                'entity_type': 'programs',
                'metric': 'revenue',
                'dimension': 'program_name',
                'title': 'Top Programs by Revenue',
                'description': 'Shows the programs that have generated the most revenue'
            },
            'top_programs_by_profit_margin': {
                'query_type': 'top',
                'entity_type': 'programs',
                'metric': 'profit_margin',
                'dimension': 'program_name',
                'title': 'Top Programs by Profit Margin',
                'description': 'Shows the programs with the highest profit margins'
            },
            'revenue_by_industry': {
                'query_type': 'distribution',
                'entity_type': 'clients',
                'metric': 'revenue',
                'dimension': 'industry',
                'title': 'Revenue Distribution by Industry',
                'description': 'Shows how revenue is distributed across different client industries'
            },
            'revenue_trend_over_time': {
                'query_type': 'trend',
                'entity_type': 'enrollments',
                'metric': 'revenue',
                'dimension': 'month',
                'title': 'Revenue Trend Over Time',
                'description': 'Shows how revenue has changed over time'
            },
            'pipeline_by_stage': {
                'query_type': 'distribution',
                'entity_type': 'opportunities',
                'metric': 'pipeline_value',
                'dimension': 'stage',
                'title': 'Pipeline Value by Stage',
                'description': 'Shows the distribution of pipeline value across different stages'
            },
            'cost_breakdown': {
                'query_type': 'distribution',
                'entity_type': 'enrollments',
                'metric': 'cost',
                'dimension': 'cost_type',
                'title': 'Cost Breakdown',
                'description': 'Shows the breakdown of costs by category'
            }
        }
    
    def simple_tokenize(self, text):
        """
        Simple tokenization function that doesn't rely on NLTK
        
        Args:
            text: Text to tokenize
            
        Returns:
            list: Tokenized words
        """
        # Convert to lowercase
        text = text.lower()
        
        # Replace punctuation with spaces
        for punct in string.punctuation:
            text = text.replace(punct, ' ')
        
        # Split on whitespace and filter out empty strings
        return [word for word in text.split() if word]
    
    def extract_number(self, text):
        """
        Extract a number from text
        
        Args:
            text: Text to extract number from
            
        Returns:
            int: Extracted number or None
        """
        # Look for patterns like "top 5", "top 10", etc.
        match = re.search(r'top\s+(\d+)', text.lower())
        if match:
            return int(match.group(1))
        
        # Look for other number patterns
        numbers = re.findall(r'\d+', text)
        if numbers:
            return int(numbers[0])
        
        return None
    
    def analyze_prompt(self, prompt):
        """
        Analyze a natural language prompt to determine the analysis parameters
        
        Args:
            prompt: Natural language prompt from user
            
        Returns:
            dict: Analysis parameters
        """
        # Default parameters
        params = {
            'query_type': 'top',
            'entity_type': 'programs',
            'metric': 'revenue',
            'dimension': 'program_name',
            'limit': 5,
            'filters': {}
        }
        
        # Tokenize the prompt
        tokens = self.simple_tokenize(prompt)
        
        # Extract limit if present
        limit = self.extract_number(prompt)
        if limit:
            params['limit'] = limit
        
        # Check for entity types
        for entity_type, keywords in self.keywords['entity_types'].items():
            for keyword in keywords:
                if keyword in prompt.lower():
                    if entity_type == 'client':
                        params['entity_type'] = 'clients'
                        params['dimension'] = 'client_name'
                    elif entity_type == 'program':
                        params['entity_type'] = 'programs'
                        params['dimension'] = 'program_name'
                    elif entity_type == 'enrollment':
                        params['entity_type'] = 'enrollments'
                    elif entity_type == 'opportunity':
                        params['entity_type'] = 'opportunities'
                    break
        
        # Check for metrics
        for metric, keywords in self.keywords['metrics'].items():
            for keyword in keywords:
                if keyword in prompt.lower():
                    if metric == 'revenue':
                        params['metric'] = 'revenue'
                    elif metric == 'profit':
                        if 'margin' in prompt.lower():
                            params['metric'] = 'profit_margin'
                        else:
                            params['metric'] = 'profit'
                    elif metric == 'cost':
                        params['metric'] = 'cost'
                    elif metric == 'count':
                        if params['entity_type'] == 'clients':
                            params['metric'] = 'client_count'
                        elif params['entity_type'] == 'programs':
                            params['metric'] = 'program_count'
                        elif params['entity_type'] == 'enrollments':
                            params['metric'] = 'enrollment_count'
                        elif params['entity_type'] == 'opportunities':
                            params['metric'] = 'opportunity_count'
                    elif metric == 'trend':
                        params['query_type'] = 'trend'
                        params['dimension'] = 'month'
                    break
        
        # Check for dimensions
        for dimension, keywords in self.keywords['dimensions'].items():
            for keyword in keywords:
                if keyword in prompt.lower():
                    if dimension == 'industry':
                        params['dimension'] = 'industry'
                    elif dimension == 'region':
                        params['dimension'] = 'region'
                    elif dimension == 'size':
                        params['dimension'] = 'size'
                    elif dimension == 'category':
                        params['dimension'] = 'category'
                    elif dimension == 'delivery_mode':
                        params['dimension'] = 'delivery_mode'
                    elif dimension == 'time':
                        params['dimension'] = 'month'
                        params['query_type'] = 'trend'
                    break
        
        # Check for query types
        for query_type, keywords in self.keywords['query_types'].items():
            for keyword in keywords:
                if keyword in prompt.lower():
                    if query_type == 'top':
                        params['query_type'] = 'top'
                    elif query_type == 'bottom':
                        params['query_type'] = 'bottom'
                    elif query_type == 'average':
                        params['query_type'] = 'average'
                    elif query_type == 'comparison':
                        params['query_type'] = 'comparison'
                    elif query_type == 'distribution':
                        params['query_type'] = 'distribution'
                    break
        
        # Check for time periods
        for period, keywords in self.keywords['time_periods'].items():
            for keyword in keywords:
                if keyword in prompt.lower():
                    if period == 'this_month':
                        params['filters']['date_from'] = 'date("now", "start of month")'
                        params['filters']['date_to'] = 'date("now")'
                    elif period == 'last_month':
                        params['filters']['date_from'] = 'date("now", "start of month", "-1 month")'
                        params['filters']['date_to'] = 'date("now", "start of month", "-1 day")'
                    elif period == 'this_quarter':
                        params['filters']['date_from'] = 'date("now", "start of month", "-" || (strftime("%m", "now") - 1) % 3 || " months")'
                        params['filters']['date_to'] = 'date("now")'
                    elif period == 'last_quarter':
                        params['filters']['date_from'] = 'date("now", "start of month", "-" || (strftime("%m", "now") - 1) % 3 + 3 || " months")'
                        params['filters']['date_to'] = 'date("now", "start of month", "-" || (strftime("%m", "now") - 1) % 3 || " months", "-1 day")'
                    elif period == 'this_year':
                        params['filters']['date_from'] = 'date("now", "start of year")'
                        params['filters']['date_to'] = 'date("now")'
                    elif period == 'last_year':
                        params['filters']['date_from'] = 'date("now", "start of year", "-1 year")'
                        params['filters']['date_to'] = 'date("now", "start of year", "-1 day")'
                    break
        
        # Check for specific filters
        # Industry filter
        industry_match = re.search(r'industry\s+(?:is|=|:)\s+([a-zA-Z\s]+)', prompt, re.IGNORECASE)
        if industry_match:
            params['filters']['industry'] = industry_match.group(1).strip()
        
        # Region filter
        region_match = re.search(r'region\s+(?:is|=|:)\s+([a-zA-Z\s]+)', prompt, re.IGNORECASE)
        if region_match:
            params['filters']['region'] = region_match.group(1).strip()
        
        # Size filter
        size_match = re.search(r'size\s+(?:is|=|:)\s+([a-zA-Z\s]+)', prompt, re.IGNORECASE)
        if size_match:
            params['filters']['size'] = size_match.group(1).strip()
        
        # Category filter
        category_match = re.search(r'category\s+(?:is|=|:)\s+([a-zA-Z\s]+)', prompt, re.IGNORECASE)
        if category_match:
            params['filters']['category'] = category_match.group(1).strip()
        
        # Delivery mode filter
        delivery_match = re.search(r'delivery\s+(?:mode|is|=|:)\s+([a-zA-Z\s]+)', prompt, re.IGNORECASE)
        if delivery_match:
            params['filters']['delivery_mode'] = delivery_match.group(1).strip()
        
        # Stage filter
        stage_match = re.search(r'stage\s+(?:is|=|:)\s+([a-zA-Z\s]+)', prompt, re.IGNORECASE)
        if stage_match:
            params['filters']['stage'] = stage_match.group(1).strip()
        
        return params
    
    def match_template(self, prompt):
        """
        Match a prompt to a predefined template
        
        Args:
            prompt: Natural language prompt from user
            
        Returns:
            dict: Template parameters or None if no match
        """
        # Check for exact matches first
        prompt_lower = prompt.lower()
        
        if 'top client' in prompt_lower and 'revenue' in prompt_lower:
            return self.templates['top_clients_by_revenue']
        
        if 'top program' in prompt_lower and 'revenue' in prompt_lower:
            return self.templates['top_programs_by_revenue']
        
        if ('top program' in prompt_lower or 'best program' in prompt_lower) and ('profit margin' in prompt_lower or 'profitability' in prompt_lower):
            return self.templates['top_programs_by_profit_margin']
        
        if 'revenue' in prompt_lower and 'industry' in prompt_lower:
            return self.templates['revenue_by_industry']
        
        if ('revenue' in prompt_lower or 'sales' in prompt_lower) and ('trend' in prompt_lower or 'over time' in prompt_lower):
            return self.templates['revenue_trend_over_time']
        
        if ('pipeline' in prompt_lower or 'opportunity' in prompt_lower) and 'stage' in prompt_lower:
            return self.templates['pipeline_by_stage']
        
        if 'cost' in prompt_lower and ('breakdown' in prompt_lower or 'distribution' in prompt_lower):
            return self.templates['cost_breakdown']
        
        # If no exact match, use the analyze_prompt function
        return None
    
    def generate_title(self, params):
        """
        Generate a title for the analysis based on parameters
        
        Args:
            params: Analysis parameters
            
        Returns:
            str: Generated title
        """
        entity_type_map = {
            'clients': 'Clients',
            'programs': 'Programs',
            'enrollments': 'Enrollments',
            'opportunities': 'Opportunities'
        }
        
        metric_map = {
            'revenue': 'Revenue',
            'profit': 'Profit',
            'profit_margin': 'Profit Margin',
            'cost': 'Cost',
            'client_count': 'Client Count',
            'program_count': 'Program Count',
            'enrollment_count': 'Enrollment Count',
            'opportunity_count': 'Opportunity Count',
            'pipeline_value': 'Pipeline Value',
            'win_rate': 'Win Rate'
        }
        
        dimension_map = {
            'client_name': '',
            'program_name': '',
            'industry': 'by Industry',
            'region': 'by Region',
            'size': 'by Size',
            'category': 'by Category',
            'delivery_mode': 'by Delivery Mode',
            'month': 'Over Time',
            'stage': 'by Stage',
            'cost_type': 'Breakdown'
        }
        
        query_type_map = {
            'top': 'Top',
            'bottom': 'Bottom',
            'average': 'Average',
            'comparison': 'Comparison of',
            'distribution': 'Distribution of',
            'trend': 'Trend of'
        }
        
        # Build title
        title_parts = []
        
        # Add query type
        if params['query_type'] in query_type_map:
            title_parts.append(query_type_map[params['query_type']])
        
        # Add limit for top/bottom queries
        if params['query_type'] in ['top', 'bottom'] and 'limit' in params:
            title_parts.append(str(params['limit']))
        
        # Add entity type for certain dimensions
        if params['dimension'] in ['client_name', 'program_name']:
            title_parts.append(entity_type_map.get(params['entity_type'], params['entity_type'].capitalize()))
        
        # Add metric
        if params['metric'] in metric_map:
            title_parts.append(metric_map[params['metric']])
        
        # Add dimension
        if params['dimension'] in dimension_map and dimension_map[params['dimension']]:
            title_parts.append(dimension_map[params['dimension']])
        
        return ' '.join(title_parts)
    
    def execute_analysis(self, params):
        """
        Execute an analysis based on parameters
        
        Args:
            params: Analysis parameters
            
        Returns:
            dict: Analysis results
        """
        try:
            # Import visualization generator
            from visualization_generator import VisualizationGenerator
            
            # Create visualization generator
            viz_gen = VisualizationGenerator(self.db_path)
            
            # Generate custom visualization
            result = viz_gen.create_custom_visualization(
                query_type=params['query_type'],
                entity_type=params['entity_type'],
                metric=params['metric'],
                dimension=params['dimension'],
                filters=params.get('filters', {}),
                limit=params.get('limit', 10)
            )
            
            # Generate title if not provided
            if 'title' not in params:
                params['title'] = self.generate_title(params)
            
            # Add parameters to result
            result['params'] = params
            
            return result
        except Exception as e:
            return {'error': str(e)}
    
    def process_prompt(self, prompt):
        """
        Process a natural language prompt and return analysis results
        
        Args:
            prompt: Natural language prompt from user
            
        Returns:
            dict: Analysis results
        """
        try:
            # Check if prompt matches a template
            template = self.match_template(prompt)
            
            if template:
                # Use template parameters
                params = template.copy()
                
                # Extract limit if present
                limit = self.extract_number(prompt)
                if limit:
                    params['limit'] = limit
            else:
                # Analyze prompt to determine parameters
                params = self.analyze_prompt(prompt)
            
            # Execute analysis
            result = self.execute_analysis(params)
            
            return result
        except Exception as e:
            return {'error': str(e)}
    
    def get_suggested_prompts(self):
        """
        Get a list of suggested prompts for the user
        
        Returns:
            list: Suggested prompts
        """
        return [
            "Show me the top 5 clients by revenue",
            "What are the most profitable programs?",
            "Show revenue trend over time",
            "What is the distribution of revenue by industry?",
            "Show me the pipeline value by stage",
            "What is the cost breakdown for all programs?",
            "Which program categories have the highest profit margins?",
            "Show me client enrollment trends over the last year",
            "What is the win rate by program category?",
            "Compare revenue and profit for different delivery modes"
        ]
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    analyzer = PromptAnalyzer()
    
    # Example: Process a prompt
    result = analyzer.process_prompt("Show me the top 5 clients by revenue")
    print("Analysis result:", result)
    
    # Example: Get suggested prompts
    suggestions = analyzer.get_suggested_prompts()
    print("Suggested prompts:", suggestions)
    
    analyzer.close()
