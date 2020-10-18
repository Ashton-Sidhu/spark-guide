import json
import math

import streamlit as st
from PIL import Image
from sympy import sympify, Eq


@st.cache()
def load_pricing_data() -> dict:

    with open("pricing_data.json") as f:
        data = json.load(f)

    return data


@st.cache()
def calc_cost_per_hour(num_nodes: int, cost: float) -> float:

    return num_nodes * cost


@st.cache()
def calc_shuffle_partition_amount(
    num_cores: int, largest_shuffle_read: int, goal_shuffle_size: int
) -> int:

    partitions = largest_shuffle_read * 1000 / goal_shuffle_size
    node_factor = math.floor(partitions / num_cores)

    return max(num_cores, node_factor * num_cores)

st.beta_set_page_config(
    page_title="Spark Guide",
    page_icon="⚡",
)

st.title("Azure Databricks Spark Guide")

# -----------------------------------------------------------------------------
# ------------------------------- Sidebar -------------------------------------
# -----------------------------------------------------------------------------

image = Image.open("images/spark.jpg")

st.sidebar.image(image, use_column_width=True)

region_select_box = st.sidebar.selectbox(
    "Select your Azure Region:", ("Canada Central",)
)

currency_selection_box = st.sidebar.selectbox("Select your Currency:", ("CAD", "USD"))


# -----------------------------------------------------------------------------
# ------------------------------- Pricing -------------------------------------
# -----------------------------------------------------------------------------

st.subheader("Cluster Cost Per Hour")

st.info("Reference: https://azure.microsoft.com/en-us/pricing/details/databricks/")

pricing_data = load_pricing_data()

node_select_box = st.selectbox("Select worker node type:", list(pricing_data.keys()))

worker_node_amount = st.number_input(
    "How many worker nodes are you using in your cluster?", min_value=1
)

total_cost = calc_cost_per_hour(
    worker_node_amount,
    pricing_data[node_select_box][region_select_box]["Total Price"],
)

st.success(f"Cost of the cluster is **${total_cost}** per hour.")


# -----------------------------------------------------------------------------
# --------------------- Shuffle Partition Calculator --------------------------
# -----------------------------------------------------------------------------

st.subheader("Shuffle Partition Calculator")
st.warning(
    "This calculation assumes you have uniformly distributed loads across your nodes."
)
st.markdown("### Formula")

str_expr_partitions = r"Partitions = \frac{LargestShuffleRead}{TargetShuffleSize}\times{1000}"
str_expr_core_factor = r"CoreCoeff = \left \lfloor {\frac{Partions}{TotalNumCores}} \right \rfloor"
str_expr_shuffle_partitions = r"ShufflePartitions = \max \left \{ TotalNumCores , CoreCoeff \times TotalNumCores \right \}"

st.latex(
    str_expr_partitions
)
st.latex(
    str_expr_core_factor
)
st.latex(
    str_expr_shuffle_partitions
)


node_shuffle_select_box = st.selectbox(
    "Select worker node type..", list(pricing_data.keys()), key="node_shuffle"
)

worker_shuffle_node_amount = st.number_input(
    "How many worker nodes are you using in your cluster?",
    min_value=1,
    key="worker_shuffle",
)

goal_shuffle_size = st.number_input(
    "Enter your target shuffle partition size (MB): ", min_value=1, key="shuffle_size"
)

largest_shuffle_read = st.number_input(
    "Enter the largest known shuffle read (GB): ", min_value=1, key="shuffle_read"
)


num_cores = (
    pricing_data[node_shuffle_select_box][region_select_box]["CPUs"]
    * worker_shuffle_node_amount
)
num_shuffle_partions = calc_shuffle_partition_amount(
    num_cores, largest_shuffle_read, goal_shuffle_size
)

st.success(
    f"Number of Shuffle Partitions you should be using is {num_shuffle_partions}"
)

st.info(
    "You can copy and paste snippet below to set the spark configuration inline."
)

st.code(f'spark.conf.set("spark.sql.shuffle_partitions", {num_shuffle_partions})')
