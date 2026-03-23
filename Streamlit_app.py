import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np

st.title("Test")
df = pd.DataFrame({"x": [1,2,3], "y": [4,5,6]})
fig = px.line(df, x="x", y="y")
st.plotly_chart(fig)

