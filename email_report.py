from airflow.utils.email import send_email
from airflow.models import TaskInstance
from airflow.utils.state import State

import base64

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time

class EmailReport:
    def __init__(self, context):
        self.dag_id = context['dag'].dag_id
        self.context = context

    def capture_dag_graph(self, dag_id):
        base_url = "baseurl of the airflow machine:8884/"
        login_url = f"{base_url}/login/"
        dag_graph_url = f"{base_url}/graph?dag_id={dag_id}"

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver', options=options)
        driver.set_window_size(1920, 1080)

        driver.get(login_url)

        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username")))
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password")))

            username = "*"
            password = "*"
            driver.find_element(By.ID, "username").send_keys(username)
            driver.find_element(By.ID, "password").send_keys(password)

            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input.btn.btn-primary.btn-block[type='submit']"))
            )
            login_button.click()

            WebDriverWait(driver, 10).until(EC.url_contains(base_url))

            driver.get(dag_graph_url)
            time.sleep(5)  # Wait for the graph to render

            svg_element = driver.find_element(By.CLASS_NAME, "output")
            png = svg_element.screenshot_as_png
        finally:
            driver.quit()

        img_str = base64.b64encode(png).decode()
        return f'<img src="data:image/png;base64,{img_str}" alt="DAG Graph">'

    def create_html_table(self):
        execution_date = self.context['execution_date']
        dag = self.context['dag']
        tasks = dag.tasks

        top_table = "<table><thead><tr><th>Task ID</th><th>Start Time</th><th>End Time</th><th>Status</th><th>Log URL</th></tr></thead><tbody>"
        top_table += "</tbody></table>"
        top_table += "<hr style='border-top: 3px solid white;'>"

        bottom_table = "<table><tbody>"

        for task in tasks:
            task_id = task.task_id
            if task_id == 'send_status_email':
                continue

            task_instance = TaskInstance(task=task, execution_date=execution_date)
            task_instance.refresh_from_db()
            status = task_instance.state or "N/A"
            start_time = task_instance.start_date.strftime("%Y-%m-%d %H:%M") if task_instance.start_date else "N/A"
            end_time = task_instance.end_date.strftime("%Y-%m-%d %H:%M") if task_instance.end_date else "N/A"
            log_url = task_instance.log_url if task_instance.execution_date else "#"

            # Apply conditional formatting for status
            status_color = {
                State.SUCCESS: 'green',
                State.FAILED: 'red',
                State.UPSTREAM_FAILED: 'orange',
            }.get(status, 'grey')

            bottom_table += f"""<tr>
                <td>{task_id}</td>
                <td>{start_time}</td>
                <td>{end_time}</td>
                <td style="color: {status_color};">{status}</td>
                <td><a href="{log_url}">Log</a></td>
                </tr>"""

        bottom_table += "</tbody></table>"

        return f"<div style='border: 3px solid #000; padding: 50px; overflow-x: auto;'>{top_table}{bottom_table}</div>"

    def send_email_report(self):
        start_time = self.context['dag_run'].start_date.strftime("%Y-%m-%d %H:%M") if self.context['dag_run'].start_date else "N/A"
        end_time = self.context['dag_run'].end_date.strftime("%Y-%m-%d %H:%M") if self.context['dag_run'].end_date else "N/A"

        subject = f"Airflow DAG {self.dag_id} - Status Update"
        graph_image = self.capture_dag_graph(self.dag_id)

        html_content = f"""
        <p style='font-size: 50; font-weight: bold;'>Airflow DAG REPORT</p>
        <p style='font-size: 30; font-weight: bold;'>DAG name: {self.dag_id}</p>
        <p style='font-size: 25; font-weight: bold;'>DAG start date: <span style='font-weight: normal;'>{start_time}</span>, DAG end date: <span style='font-weight: normal;'>{end_time}</span></p>
        <p style='font-size: 35; font-weight: bold;'>DAG task detail:</p>
        {self.create_html_table()}
        <p style='font-size: 35; font-weight: bold;'>DAG Graph View:</p>
        {graph_image}
        """

        if any(t.state == State.FAILED for t in self.context['dag_run'].get_task_instances()):
            html_content += f"<p style='font-size: 30; font-weight: bold; font-color: white; background-color: red; padding: 10px;'>DAG ERROR! CHECK {self.dag_id} DAG!</p>"
        else:
            html_content += f"<p style='font-size: 30; font-weight: bold; font-color: white; background-color: green; padding: 10px;'>{self.dag_id} DAG Finished Successfully</p>"

        to_email = "*"
        send_email(to=to_email, subject=subject, html_content=html_content)
