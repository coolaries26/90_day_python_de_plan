@echo off
echo ============================================
echo  E-Commerce Analytics Dashboard
echo  Python DE Journey — Capstone
echo ============================================
cd /d C:\90_day_python_de_plan
call .venv\Scripts\activate
streamlit run capstone/dashboard/app.py --server.port 8502 ^
    --browser.gatherUsageStats false
pause