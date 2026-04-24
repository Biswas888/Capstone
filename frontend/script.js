document.addEventListener("DOMContentLoaded", () => {
    const API_BASE = "http://127.0.0.1:5050";
    console.log("script.js loaded");

    // Sections
    const homeSection = document.getElementById("home-section");
    const dataSection = document.getElementById("data-section");
    const predictSection = document.getElementById("predict-section");

    // Nav links
    const navHome = document.getElementById("nav-home");
    const navData = document.getElementById("nav-data");
    const navPredict = document.getElementById("nav-predict");

    // Buttons
    const goDataBtn = document.getElementById("goDataBtn");
    const goPredictBtn = document.getElementById("goPredictBtn");
    const goPredictBtn2 = document.getElementById("goPredictBtn2");

    const fetchBtn = document.getElementById("fetchBtn");
    const combinedDiv = document.getElementById("combinedData");

    const uploadPredictBtn = document.getElementById("uploadPredictBtn");
    const csvFileInput = document.getElementById("csvFile");
    const csvCityInput = document.getElementById("csvCityInput");
    const predictionResultsDiv = document.getElementById("predictionResults");

    const forecastResults = document.getElementById("forecastResults");
    const summaryCards = document.getElementById("summaryCards");
    const insightList = document.getElementById("insightList");
    const topItemsDiv = document.getElementById("topItems");
    const downloadLink = document.getElementById("downloadLink");
    const forecastChartCanvas = document.getElementById("forecastChart");

    const categorySummaryTable = document.getElementById("categorySummaryTable");
    const itemSummaryTable = document.getElementById("itemSummaryTable");
    const revenueChartCanvas = document.getElementById("revenueChart");

    const visualizeBtn = document.getElementById("visualizeDashboardBtn");
    const metabaseContainer = document.getElementById("metabaseContainer");
    const metabaseFrame = document.getElementById("metabaseFrame");

    function showSection(section) {
        if (homeSection) homeSection.classList.add("hidden");
        if (dataSection) dataSection.classList.add("hidden");
        if (predictSection) predictSection.classList.add("hidden");

        if (navHome) navHome.classList.remove("active");
        if (navData) navData.classList.remove("active");
        if (navPredict) navPredict.classList.remove("active");

        if (section === "home") {
            if (homeSection) homeSection.classList.remove("hidden");
            if (navHome) navHome.classList.add("active");
        } else if (section === "data") {
            if (dataSection) dataSection.classList.remove("hidden");
            if (navData) navData.classList.add("active");
        } else if (section === "predict") {
            if (predictSection) predictSection.classList.remove("hidden");
            if (navPredict) navPredict.classList.add("active");
        }
    }

    function setActiveNav(activeId) {
        if (navHome) navHome.classList.remove("active");
        if (navData) navData.classList.remove("active");
        if (navPredict) navPredict.classList.remove("active");

        const activeLink = document.getElementById(activeId);
        if (activeLink) activeLink.classList.add("active");
    }

    if (navHome) {
        navHome.addEventListener("click", (e) => {
            e.preventDefault();
            showSection("home");
        });
    }

    if (navData) {
        navData.addEventListener("click", (e) => {
            e.preventDefault();
            showSection("data");
        });
    }

    if (navPredict) {
        navPredict.addEventListener("click", (e) => {
            e.preventDefault();
            showSection("predict");
        });
    }

    if (goDataBtn) {
        goDataBtn.addEventListener("click", () => showSection("data"));
    }

    if (goPredictBtn) {
        goPredictBtn.addEventListener("click", () => showSection("predict"));
    }

    if (goPredictBtn2) {
        goPredictBtn2.addEventListener("click", () => showSection("predict"));
    }

    // -----------------------------
    // Metabase dashboard button
    // -----------------------------
    if (visualizeBtn) {
        visualizeBtn.addEventListener("click", async () => {
            try {
                const res = await fetch(`${API_BASE}/api/get-metabase-token`);
                const data = await res.json();

                const token = data.token;
                const iframeUrl = `http://localhost:3000/embed/dashboard/${token}#bordered=false&titled=false`;

                if (metabaseFrame) {
                    metabaseFrame.src = iframeUrl;
                }

                if (metabaseContainer) {
                    metabaseContainer.classList.remove("hidden");
                }
            } catch (err) {
                console.error("Metabase load error:", err);
            }
        });
    }

    // -----------------------------
    // Helpers
    // -----------------------------
    function formatNumber(value, digits = 2) {
        const num = Number(value);
        return Number.isFinite(num) ? num.toFixed(digits) : "-";
    }

    function getDemandClass(predictedSales) {
        const value = Number(predictedSales) || 0;
        if (value >= 20) return "high-demand";
        if (value >= 5) return "moderate-demand";
        return "low-demand";
    }

    function safeText(value, fallback = "-") {
        return value !== null && value !== undefined && value !== "" ? value : fallback;
    }

    // -----------------------------
    // Render historical combined data
    // -----------------------------
    function renderCombinedData(data) {
        if (!combinedDiv) return;

        if (!data || data.length === 0) {
            combinedDiv.innerHTML = `<p class="placeholder-text">No data found in the database.</p>`;
            return;
        }

        let html = `
            <table border="1" cellpadding="8" style="border-collapse: collapse; width: 100%; text-align: left;">
                <tr style="background-color: #f2f2f2;">
                    <th>Date</th>
                    <th>Total Quantity</th>
                    <th>Total Revenue</th>
                    <th>Avg Temp (°C)</th>
                    <th>Humidity (%)</th>
                    <th>Rainfall (mm)</th>
                    <th>Forecast Type</th>
                </tr>
        `;

        data.forEach(row => {
            html += `
                <tr>
                    <td>${safeText(row.date)}</td>
                    <td>${safeText(row.total_quantity)}</td>
                    <td>${row.total_revenue != null ? "$" + formatNumber(row.total_revenue) : "-"}</td>
                    <td>${safeText(row.avg_temp)}</td>
                    <td>${safeText(row.humidity)}</td>
                    <td>${safeText(row.rainfall)}</td>
                    <td>${safeText(row.forecast_type)}</td>
                </tr>
            `;
        });

        html += "</table>";
        combinedDiv.innerHTML = html;
    }

    // -----------------------------
    // Render CSV prediction summary cards
    // -----------------------------
    function renderSummaryCards(summary) {
        if (!summaryCards || !summary) return;

        summaryCards.innerHTML = `
            <div class="summary-card">
                <h4>Total Rows</h4>
                <p>${safeText(summary.total_rows, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Rows Predicted</h4>
                <p>${safeText(summary.rows_with_predictions, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Total Forecast Sales</h4>
                <p>${safeText(summary.total_predicted_sales, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Average Daily Sales</h4>
                <p>${safeText(summary.average_predicted_sales, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Total Revenue</h4>
                <p>${safeText(summary.total_predicted_revenue, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Average Revenue</h4>
                <p>${safeText(summary.average_predicted_revenue, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Peak Sales Date</h4>
                <p>${safeText(summary.peak_date)}</p>
            </div>
            <div class="summary-card">
                <h4>Peak Sales</h4>
                <p>${safeText(summary.peak_sales, 0)}</p>
            </div>
            <div class="summary-card">
                <h4>Peak Revenue Date</h4>
                <p>${safeText(summary.peak_revenue_date)}</p>
            </div>
            <div class="summary-card">
                <h4>Peak Revenue</h4>
                <p>${safeText(summary.peak_revenue, 0)}</p>
            </div>
        `;
    }

    function renderInsights(insights) {
        if (!insightList) return;

        if (!insights || insights.length === 0) {
            insightList.innerHTML = "<li>No insights available.</li>";
            return;
        }

        insightList.innerHTML = insights.map(item => `<li>${item}</li>`).join("");
    }

    function renderTopItems(topItems) {
        if (!topItemsDiv) return;

        if (!topItems || topItems.length === 0) {
            topItemsDiv.innerHTML = "<p>No item-level insights available.</p>";
            return;
        }

        topItemsDiv.innerHTML = topItems.map(item => `
            <div class="top-item">
                <strong>${safeText(item.item_id)}</strong> — Total Predicted Sales: ${formatNumber(item.total_predicted_sales)}
            </div>
        `).join("");
    }



    function renderCsvForecastResults(result) {
        window.scrollTo({ top: 0, behavior: "smooth" });

        if (predictionResultsDiv) {
            predictionResultsDiv.innerHTML = `
                <p class="success-text">${safeText(result.message, "Prediction completed successfully.")}</p>
            `;
        }

        if (forecastResults) {
            forecastResults.classList.remove("hidden");
        }

        sessionStorage.setItem("latestPredictionPayload", JSON.stringify(result));

        renderSummaryCards(result.summary);
        renderInsights(result.insights);
        renderTopItems(result.top_items);
        renderPredictionTable(result.preview);
        renderForecastChart(result.chart_data);
        renderCategorySummary(result.category_summary);
        renderItemSummary(result.item_summary);
        renderRevenueChart(result.revenue_over_time);
        renderInventorySummary(result.inventory_summary || []);

        if (downloadLink) {
            if (result.download_url) {
                downloadLink.href = `${API_BASE}${result.download_url}`;
                downloadLink.style.display = "inline-block";
            } else {
                downloadLink.style.display = "none";
            }
        }
    }

    //render category summary 
    function renderCategorySummary(categorySummary) {
        if (!categorySummaryTable) return;

        if (!categorySummary || categorySummary.length === 0) {
            categorySummaryTable.innerHTML = "<p>No category summary available.</p>";
            return;
        }

        let html = `
            <table>
                <thead>
                    <tr>
                        <th>Category</th>
                        <th>Total Sales</th>
                        <th>Average Sales</th>
                        <th>Total Revenue</th>
                        <th>Unique Items</th>
                    </tr>
                </thead>
                <tbody>
        `;

        categorySummary.forEach(row => {
            html += `
                <tr>
                    <td>${safeText(row.category)}</td>
                    <td>${safeText(row.total_predicted_sales)}</td>
                    <td>${safeText(row.average_predicted_sales)}</td>
                    <td>${safeText(row.total_predicted_revenue)}</td>
                    <td>${safeText(row.item_count)}</td>
                </tr>
            `;
        });

        html += `
                </tbody>
            </table>
        `;

        categorySummaryTable.innerHTML = html;
    }

    //render item summary
    function renderItemSummary(itemSummary) {
        if (!itemSummaryTable) return;

        if (!itemSummary || itemSummary.length === 0) {
            itemSummaryTable.innerHTML = "<p>No item summary available.</p>";
            return;
        }

        let html = `
            <table>
                <thead>
                    <tr>
                        <th>Item ID</th>
                        <th>Category</th>
                        <th>Total Sales</th>
                        <th>Average Sales</th>
                        <th>Total Revenue</th>
                        <th>Avg Sell Price</th>
                    </tr>
                </thead>
                <tbody>
        `;

        itemSummary.forEach(row => {
            html += `
                <tr>
                    <td>${safeText(row.item_id)}</td>
                    <td>${safeText(row.category)}</td>
                    <td>${safeText(row.total_predicted_sales)}</td>
                    <td>${safeText(row.average_predicted_sales)}</td>
                    <td>${safeText(row.total_predicted_revenue)}</td>
                    <td>${safeText(row.avg_sell_price)}</td>
                </tr>
            `;
        });

        html += `
                </tbody>
            </table>
        `;

        itemSummaryTable.innerHTML = html;
    }

    //render revenue chart
    function renderItemSummary(itemSummary) {
        if (!itemSummaryTable) return;

        if (!itemSummary || itemSummary.length === 0) {
            itemSummaryTable.innerHTML = "<p>No item summary available.</p>";
            return;
        }

        let html = `
            <table>
                <thead>
                    <tr>
                        <th>Item ID</th>
                        <th>Category</th>
                        <th>Total Sales</th>
                        <th>Average Sales</th>
                        <th>Total Revenue</th>
                        <th>Avg Sell Price</th>
                    </tr>
                </thead>
                <tbody>
        `;

        itemSummary.forEach(row => {
            html += `
                <tr>
                    <td>${safeText(row.item_id)}</td>
                    <td>${safeText(row.category)}</td>
                    <td>${safeText(row.total_predicted_sales)}</td>
                    <td>${safeText(row.average_predicted_sales)}</td>
                    <td>${safeText(row.total_predicted_revenue)}</td>
                    <td>${safeText(row.avg_sell_price)}</td>
                </tr>
            `;
        });

        html += `
                </tbody>
            </table>
        `;

        itemSummaryTable.innerHTML = html;
    }

    // -----------------------------
    // CSV + forecast prediction
    // -----------------------------
    if (uploadPredictBtn) {
        uploadPredictBtn.addEventListener("click", async (e) => {
            e.preventDefault();
            const file = csvFileInput?.files?.[0];
            const city = csvCityInput?.value?.trim();

            if (!file) {
                if (predictionResultsDiv) {
                    predictionResultsDiv.innerHTML = `<p class="error-text">Please choose a CSV file first.</p>`;
                }
                return;
            }

            if (!city) {
                if (predictionResultsDiv) {
                    predictionResultsDiv.innerHTML = `<p class="error-text">Please enter a city.</p>`;
                }
                return;
            }

            const formData = new FormData();
            formData.append("file", file);
            formData.append("city", city);

            if (predictionResultsDiv) {
                predictionResultsDiv.innerHTML = `<div class="loading">Uploading CSV and generating predictions...</div>`;
            }

            if (forecastResults) {
                forecastResults.classList.add("hidden");
            }

            try {
                const response = await fetch(`${API_BASE}/api/predict-csv-with-forecast`, {
                    method: "POST",
                    body: formData
                });

                const result = await response.json();
                console.log("CSV forecast result:", result);
                updateHeroMetricsFromPrediction(result);

                if (!response.ok) {
                    if (predictionResultsDiv) {
                        predictionResultsDiv.innerHTML = `
                            <p class="error-text">Prediction failed.</p>
                            <pre>${JSON.stringify(result, null, 2)}</pre>
                        `;
                    }
                    return;
                }

                console.log("POST succeeded, about to show predict section");
                showSection("predict");
                setActiveNav("nav-predict");
                console.log("predict section should now be visible");

                renderCsvForecastResults(result);
                console.log("renderCsvForecastResults completed");
            } catch (error) {
                console.error("Prediction fetch error:", error);
                if (predictionResultsDiv) {
                    predictionResultsDiv.innerHTML = `<p class="error-text">Error: ${error.message}</p>`;
                }
            }
        });
    }

    //restore previous prediction from session (if exists)
    const savedData = restorePredictionFromSession();
    if (savedData) {
        showSection("predict");
        setActiveNav("nav-predict");

        if (forecastResults) {
            forecastResults.classList.remove("hidden");
        }

        renderSummaryCards(savedData.summary);
        renderInsights(savedData.insights);
        renderTopItems(savedData.top_items);
        renderPredictionTable(savedData.preview);
        renderForecastChart(savedData.chart_data);
        renderCategorySummary(savedData.category_summary);
        renderItemSummary(savedData.item_summary);
        renderRevenueChart(savedData.revenue_over_time);
        updateHeroMetricsFromPrediction(savedData);
    }
});
