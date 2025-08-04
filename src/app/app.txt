import streamlit as st
from databricks.sdk import WorkspaceClient

import duckdb

w = WorkspaceClient()


def test_db():
    parq_path = "/Users/bryan/dev/kingfisher_wells/src/well_surface_locations/"
    parq_path += "part-00000-tid-7502051727140489421-55d6fc8a-bdbb-40db-8cda-bf1cf862ebe2-0-1.c000.snappy.parquet"
    df = duckdb.sql(f"select * from '{parq_path}'").df()
    st.text(df.describe())


@st.cache_data
def get_job_ids():
    """Get job IDs by name matching and cache them"""
    try:
        jobs = list(w.jobs.list())
        job_ids = {}

        for job in jobs:
            name = job.settings.name
            # Match jobs containing our base names
            if "Main Job" in name:
                job_ids["main"] = job.job_id
                job_ids["main_name"] = name
            elif "Well Job" in name:
                job_ids["well"] = job.job_id
                job_ids["well_name"] = name

        return job_ids
    except Exception as e:
        st.error(f"Error getting job IDs: {str(e)}")
        return {}


def trigger_job_by_id(job_id, job_name="Job"):
    """Trigger a job using its ID"""
    try:
        response = w.jobs.run_now(job_id=job_id)
        st.success(f"{job_name} started successfully! Run ID: {response.run_id}")
        return response
    except Exception as e:
        st.error(f"Failed to trigger {job_name}: {str(e)}")
        return None


# Get job IDs once and cache them
job_ids = get_job_ids()

# UI
st.title("Databricks Job Trigger")

test_db()

if job_ids:
    col1, col2 = st.columns(2)

    with col1:
        if "main" in job_ids:
            if st.button("Trigger Main Job", use_container_width=True):
                trigger_job_by_id(job_ids["main"], "Main Job")
        else:
            st.error("Main Job not found")

    with col2:
        if "well" in job_ids:
            if st.button("Trigger Well Job", use_container_width=True):
                trigger_job_by_id(job_ids["well"], "Well Job")
        else:
            st.error("Well Job not found")

    # Debug section
    with st.expander("Debug: Job Information"):
        if "main" in job_ids:
            st.write(
                f"Main Job ID: {job_ids['main']} (Name: {job_ids.get('main_name', 'Unknown')})"
            )
        if "well" in job_ids:
            st.write(
                f"Well Job ID: {job_ids['well']} (Name: {job_ids.get('well_name', 'Unknown')})"
            )
else:
    st.error("No jobs found. Check service principal permissions.")
