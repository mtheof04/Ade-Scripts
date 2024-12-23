# Ade-Scripts

<p>This README outlines the setup, execution, and queries used in three experiments to evaluate performance and energy efficiency in data analysis systems.</p>



<h2>Experiment 1</h2>
<p>This experiment focuses on data import for the TPC-H benchmark:</p>
<ul>
    <li><strong>Scale Factors (SF)</strong>: Data was generated for scale factors of 100 and 300, corresponding to approximately 100 GB and 300 GB of data.</li>
    <li><strong>Objective</strong>: Compare the energy consumption and data loading speed of the CSV and Parquet formats in DuckDB under cold start conditions.</li>
</ul>

<hr>

<h2>Experiment 2</h2>
<p>This experiment evaluates the performance of five main query categories using the TPC-H benchmark:</p>
<ul>
    <li><strong>Sequential</strong></li>
    <li><strong>Filtering</strong></li>
    <li><strong>Aggregation</strong></li>
    <li><strong>Sorting</strong></li>
    <li><strong>Join</strong></li>
</ul>

<hr>

<h2>Experiment 3</h2>
<p>This experiment focuses on executing all 22 queries of the TPC-H benchmark to evaluate DuckDB's query performance:</p>
<ul>
     <li>Identify the queries with the most and least energy consumption.</li>
</ul>

<hr>
