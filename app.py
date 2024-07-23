from flask import Flask, render_template, request
import pandas as pd
import numpy as np
import pyarrow as pa
import fastparquet as fp
import json
import os

app = Flask(__name__)

def find_top_sources(df, country_target, item):
    
    country_target = country_target.lower()
    item = item.lower()

    # Filter the DataFrame for the specified country and item
    filtered_df = df[(df['Country_target'] == country_target) & (df['Item'] == item)]
    
    if filtered_df.empty:
        return None, 0, 0

    # Calculate the total weight of the item imported from each source country
    total_weight = filtered_df['Value_tons'].sum()
    
    # Calculate the total weight of the item imported from each source country
    source_weights = filtered_df.groupby('Country_source')['Value_tons'].sum().sort_values(ascending=False)
    
    # Calculate the probability for each Country_source based on the weight
    source_probabilities = source_weights / total_weight
    
    # Filter for countries with individual probability higher than 20%
    top_sources = source_probabilities[source_probabilities > 0.20].head(3)
    
    if top_sources.empty:
        return None, 0, 0

    # Check if all distances for the top sources are less than 2000 km
    top_countries = top_sources.index
    top_distances = filtered_df[filtered_df['Country_source'].isin(top_countries)]['distance_in_km']
    
    if (top_distances < 2000).all() or country_target in top_countries:
        print("GOOD! You are eating local")
        return None, 0, 0

    # Calculate the summed probability of the selected countries
    summed_probability = top_sources.sum()
    
    # Calculate the mean CO2 emissions for the selected countries
    mean_co2_emissions = filtered_df[filtered_df['Country_source'].isin(top_countries)]['kgCO2eq_tkm'].mean()
    
    return top_sources, summed_probability, mean_co2_emissions


@app.route('/')
def home():
    with open('./data/food_items.json', 'r') as f:
        food_items = json.load(f)['food_items']
    return render_template('home.html', food_items=food_items)

@app.route('/result', methods=['POST'])
def result():
    # Retrieve form data
    country_target = request.form['location']
    item = request.form['food']
    
    # Load the data from the CSV file
    df_path = './data/trade_mx_app.parquet'
    if os.path.exists(df_path):
        df = pd.read_parquet(df_path)
    else:
        # Handle the case where the file does not exist
        return "Data file not found.", 404

    # Call the function with the provided inputs
    top_sources, summed_probability, mean_co2 = find_top_sources(df, country_target, item)
    
    # Prepare results for display
    if top_sources is not None:
        if len(top_sources) == 3:
            sources_text = f"{top_sources.index[0]}, {top_sources.index[1]} or {top_sources.index[2]}"
        elif len(top_sources) == 2:
            sources_text = f"{top_sources.index[0]} or {top_sources.index[1]}"
        else:  # len(top_sources) == 1
            sources_text = top_sources.index[0]
        
        sources_text = sources_text.upper()
        probability_text = f"With a probability of {summed_probability*100:.2f}%, your food is coming from {sources_text}."
        
        co2_amount = mean_co2 / 1e6
        if co2_amount < 1:
            co2_text = f"Generating approximately {co2_amount:.2f} million Kg of CO2, which is not that bad!"
            co2_class = "co2-text-yellow"
        else:
            co2_text = f"That generates approximately {co2_amount:.2f} million Kg of CO2!!"
            co2_class = "co2-text-red"
    else:
        probability_text = "GOOD! You are eating local."
        co2_text = ""
        co2_class = "co2-text-green"

    # Render the result template with the results
    return render_template('result.html', probability_text=probability_text, co2_text=co2_text, co2_class=co2_class)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000, debug=True)

