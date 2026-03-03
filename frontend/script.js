document.addEventListener("DOMContentLoaded", () => {
    const fetchBtn = document.getElementById("fetchBtn");
    const combinedDiv = document.getElementById("combinedData");
    const predictionDiv = document.getElementById("prediction");

    console.log("Frontend JS loaded. Ready to fetch data from Flask.");

    fetchBtn.addEventListener("click", () => {
        console.log("Fetch button clicked. Requesting data...");
        
        // Show a loading message
        combinedDiv.innerHTML = "<p>Loading data from database...</p>";
        predictionDiv.innerHTML = "<p>Calculating prediction...</p>";

        // 1️⃣ Combined Sales + Weather Data
        fetch("/api/combined")
            .then(res => {
                if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                return res.json();
            })
            .then(data => {
                if (data.error) {
                    combinedDiv.innerHTML = `<p style="color: red;">Error: ${data.error}</p>`;
                    return;
                }

                if (!data || data.length === 0) {
                    combinedDiv.innerHTML = "<p>No data found in the database.</p>";
                    return;
                }

                let html = `<table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%; text-align: left;">
                                <tr style="background-color: #f2f2f2;">
                                    <th>Date</th>
                                    <th>Total Quantity</th>
                                    <th>Total Revenue</th>
                                    <th>Avg Temp (°C)</th>
                                    <th>Humidity (%)</th>
                                    <th>Rainfall (mm)</th>
                                    <th>Forecast Type</th>
                                </tr>`;

                data.forEach(row => {
                    html += `<tr>
                                <td>${row.date}</td>
                                <td>${row.total_quantity}</td>
                                <td>$${parseFloat(row.total_revenue).toFixed(2)}</td>
                                <td>${row.avg_temp ?? "-"}</td>
                                <td>${row.humidity ?? "-"}</td>
                                <td>${row.rainfall ?? "-"}</td>
                                <td>${row.forecast_type ?? "-"}</td>
                             </tr>`;
                });

                html += "</table>";
                combinedDiv.innerHTML = html;
            })
            .catch(err => {
                combinedDiv.innerHTML = `<p style="color: red;">Failed to connect to API: ${err.message}</p>`;
                console.error("Combined API Error:", err);
            });

        // 2️⃣ Sales Prediction Logic
        fetch("/api/prediction_input")
            .then(res => {
                if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                return res.json();
            })
            .then(data => {
                if (data.error) {
                    predictionDiv.innerHTML = `<p style="color: red;">Error: ${data.error}</p>`;
                    return;
                }

                const latestSales = data.latest_sales_features;
                const weather = data.current_weather;
                
                // Demo logic: If it's hot (>25C), boost predicted sales by 20%
                let predictedNextSales = latestSales.total_quantity;
                if (weather.temperature > 25) {
                    predictedNextSales = Math.round(predictedNextSales * 1.2);
                }

                predictionDiv.innerHTML = `
                    <div style="border: 1px solid #ccc; padding: 15px; border-radius: 5px; background-color: #f9f9f9;">
                        <p><strong>Latest Sales Date:</strong> ${latestSales.date}</p>
                        <p><strong>Actual Last Quantity:</strong> ${latestSales.total_quantity}</p>
                        <hr>
                        <p><strong>Current Weather Context:</strong> ${weather.temperature}°C, ${weather.humidity}% Humidity</p>
                        <p style="font-size: 1.2em; color: #2c3e50;"><strong>Predicted Next Sales:</strong> ${predictedNextSales} units</p>
                    </div>
                `;
            })
            .catch(err => {
                predictionDiv.innerHTML = `<p style="color: red;">Failed to connect to Prediction API</p>`;
                console.error("Prediction API Error:", err);
            });
    });
});