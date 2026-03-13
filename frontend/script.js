document.addEventListener("DOMContentLoaded", () => {
    // Navigation Logic
    const sections = {
        home: document.getElementById('home-section'),
        data: document.getElementById('data-section'),
        predict: document.getElementById('predict-section')
    };

    function showSection(id) {
        Object.values(sections).forEach(s => s.classList.add('hidden'));
        sections[id].classList.remove('hidden');
    }

    document.getElementById('nav-home').addEventListener('click', (e) => { e.preventDefault(); showSection('home'); });
    document.getElementById('nav-data').addEventListener('click', (e) => { e.preventDefault(); showSection('data'); });
    document.getElementById('nav-predict').addEventListener('click', (e) => { e.preventDefault(); showSection('predict'); });

    const fetchBtn = document.getElementById("fetchBtn");
    const combinedDiv = document.getElementById("combinedData");
    const predictionResultsDiv = document.getElementById("predictionResults");

    fetchBtn.addEventListener("click", () => {
        // Show loading states
        combinedDiv.innerHTML = "<p>Loading data from database...</p>";
        predictionResultsDiv.innerHTML = "<p>Calculating 7-day forecast...</p>";

        // --- 1. YOUR ORIGINAL COMBINED DATA LOGIC ---
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
                
                // Automatically show the data section after fetching
                showSection('data');
            })
            .catch(err => {
                combinedDiv.innerHTML = `<p style="color: red;">Failed to connect to API: ${err.message}</p>`;
            });

        // --- 2. NEW 7-DAY PREDICTION LOGIC ---
        fetch("/api/prediction_input")
            .then(res => {
                if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                return res.json();
            })
            .then(data => {
                if (data.error) {
                    predictionResultsDiv.innerHTML = `<p style="color: red;">Error: ${data.error}</p>`;
                    return;
                }

                const latestSales = data.latest_sales_features;
                const weather = data.current_weather;
                let forecastHtml = "";

                // Generate 7 days of predictions
                for (let i = 1; i <= 7; i++) {
                    // Logic: Base quantity from last known day + slight random variation for demo
                    let predictedQty = latestSales.total_quantity;
                    
                    // Apply your weather boost logic (>25C)
                    // We simulate temperature changing slightly over the week
                    let simulatedTemp = weather.temperature + (i * 0.5); 
                    
                    if (simulatedTemp > 25) {
                        predictedQty = Math.round(predictedQty * 1.2);
                    }

                    forecastHtml += `
                        <div class="forecast-card">
                            <h3>Day +${i}</h3>
                            <p class="prediction-value"><strong>${predictedQty}</strong> Units</p>
                            <hr>
                            <small>Temp: ${simulatedTemp.toFixed(1)}°C</small><br>
                            <small>Hum: ${weather.humidity}%</small>
                        </div>
                    `;
                }
                predictionResultsDiv.innerHTML = forecastHtml;
            })
            .catch(err => {
                predictionResultsDiv.innerHTML = `<p style="color: red;">Failed to connect to Prediction API</p>`;
            });
    });
});