# Streamlit Cloud Deployment Guide

This guide provides step-by-step instructions for deploying the Teaching Organization Analytics application to Streamlit Cloud for permanent hosting.

## Prerequisites

1. GitHub account
2. Streamlit Cloud account (free tier available)

## Deployment Steps

### 1. Create a GitHub Repository

1. Go to [GitHub](https://github.com) and sign in to your account
2. Click the "+" icon in the top-right corner and select "New repository"
3. Name your repository (e.g., "teaching-analytics")
4. Set the repository to Public (required for free Streamlit Cloud hosting)
5. Click "Create repository"
6. Follow the instructions to push an existing repository from the command line:
   ```
   git remote add origin https://github.com/YOUR-USERNAME/teaching-analytics.git
   git branch -M main
   git push -u origin main
   ```

### 2. Deploy to Streamlit Cloud

1. Go to [Streamlit Cloud](https://streamlit.io/cloud)
2. Sign in with your GitHub account
3. Click "New app"
4. Select your repository, branch (main), and the main file path (app.py)
5. Click "Deploy"

Your application will be deployed and available at:
`https://YOUR-USERNAME-teaching-analytics.streamlit.app`

### 3. Verify Deployment

1. Wait for the deployment to complete (usually takes 1-2 minutes)
2. Visit your application URL
3. Test the drag-and-drop data import functionality
4. Navigate through all analysis modules to ensure they're working correctly

### 4. Custom Domain (Optional)

If you want to use a custom domain:

1. Go to your app settings in Streamlit Cloud
2. Click "Custom domain"
3. Follow the instructions to set up DNS records for your domain

## Troubleshooting

If you encounter any issues during deployment:

1. Check the deployment logs in Streamlit Cloud
2. Ensure all dependencies are listed in requirements.txt
3. Verify that your app.py file is in the root directory of your repository
4. Make sure your GitHub repository is public

## Maintenance

To update your application:

1. Make changes to your local repository
2. Commit and push to GitHub
3. Streamlit Cloud will automatically redeploy your application

## Support

For additional help:
- Streamlit Documentation: https://docs.streamlit.io/
- Streamlit Community Forum: https://discuss.streamlit.io/
