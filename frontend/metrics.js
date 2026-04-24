function safeText(value, fallback = "-") {
  return value !== null && value !== undefined && value !== "" ? value : fallback;
}

function getDemandClass(predictedSales) {
  const value = Number(predictedSales) || 0;
  if (value >= 20) return "high-demand";
  if (value >= 5) return "moderate-demand";
  return "low-demand";
}

function restorePredictionFromSession() {
  const saved = sessionStorage.getItem("latestPredictionPayload");

  if (!saved) return null;

  try {
    return JSON.parse(saved);
  } catch (e) {
    console.error("Failed to parse saved prediction:", e);
    return null;
  }
}

function updateHeroMetricsFromPrediction(payload) {
  const totalForecastEl = document.getElementById("totalForecastMetric");
  const weatherImpactEl = document.getElementById("weatherImpactMetric");
  const topCategoryEl = document.getElementById("topCategoryMetric");

  const rows = Array.isArray(payload?.preview) ? payload.preview : [];
  const summary = payload?.summary || {};
  const categorySummary = Array.isArray(payload?.category_summary)
    ? payload.category_summary
    : [];

  if (totalForecastEl) {
    const totalPredictedSales = Number(summary.total_predicted_sales || 0);
    const historicalTotalSales = Number(summary.historical_total_sales || 0);

    let totalForecastText = "--";

    if (historicalTotalSales > 0) {
      const pctChange =
        ((totalPredictedSales - historicalTotalSales) / historicalTotalSales) * 100;
      const sign = pctChange >= 0 ? "+" : "";
      totalForecastText = `${sign}${pctChange.toFixed(1)}%`;
    } else if (totalPredictedSales > 0) {
      totalForecastText = `${totalPredictedSales.toFixed(1)} units`;
    }

    totalForecastEl.textContent = totalForecastText;
  }

  if (weatherImpactEl) {
    let weatherImpactText = "Low";

    if (rows.length > 0) {
      const temps = rows.map(r => Number(r.temperature_2m_mean || 0));
      const rain = rows.map(r => Number(r.rain_sum || 0));
      const preds = rows.map(r => Number(r.predicted_sales || 0));

      const tempRange = Math.max(...temps) - Math.min(...temps);
      const totalRain = rain.reduce((sum, v) => sum + v, 0);
      const salesRange = Math.max(...preds) - Math.min(...preds);

      if (tempRange > 10 || totalRain > 10 || salesRange > 10) {
        weatherImpactText = "High";
      } else if (tempRange > 5 || totalRain > 5) {
        weatherImpactText = "Moderate";
      }
    }

    weatherImpactEl.textContent = weatherImpactText;
  }

  if (topCategoryEl) {
    topCategoryEl.textContent =
      categorySummary.length > 0 ? safeText(categorySummary[0].category) : "--";
  }
}

function renderInventorySummary(inventorySummary = []) {
  const tableBody = document.querySelector("#inventoryTable tbody");
  const cardsContainer = document.getElementById("inventorySummaryCards");

  if (!tableBody) return;

  if (!inventorySummary.length) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="8">No inventory insights available.</td>
      </tr>
    `;
    if (cardsContainer) cardsContainer.innerHTML = "";
    return;
  }

  const highRisk = inventorySummary.filter(i => i.urgency === "High").length;
  const mediumRisk = inventorySummary.filter(i => i.urgency === "Medium").length;
  const totalReorder = inventorySummary.reduce(
    (sum, item) => sum + (Number(item.recommended_reorder) || 0),
    0
  );

  if (cardsContainer) {
    cardsContainer.innerHTML = `
      <div class="mini-card">
        <h4>High Risk Items</h4>
        <p>${highRisk}</p>
      </div>
      <div class="mini-card">
        <h4>Medium Risk Items</h4>
        <p>${mediumRisk}</p>
      </div>
      <div class="mini-card">
        <h4>Total Reorder Qty</h4>
        <p>${totalReorder.toFixed(2)}</p>
      </div>
    `;
  }

  tableBody.innerHTML = inventorySummary.map(item => {
    const urgency = item.urgency || "Low";
    const badgeClass =
      urgency === "High"
        ? "risk-high"
        : urgency === "Medium"
        ? "risk-medium"
        : "risk-low";

    return `
      <tr>
        <td>${safeText(item.item_id)}</td>
        <td>${safeText(item.category)}</td>
        <td>${safeText(item.estimated_stock)}</td>
        <td>${safeText(item.historical_avg_daily_sales)}</td>
        <td>${safeText(item.predicted_avg_daily_demand)}</td>
        <td>${safeText(item.days_until_stockout)}</td>
        <td>${safeText(item.recommended_reorder)}</td>
        <td><span class="risk-badge ${badgeClass}">${safeText(item.urgency)}</span></td>
      </tr>
    `;
  }).join("");
}

function renderPredictionTable(preview = []) {
  const predictionTable = document.getElementById("predictionTable");

  if (!predictionTable) return;

  if (!Array.isArray(preview) || preview.length === 0) {
    predictionTable.innerHTML = "<p>No preview data available.</p>";
    return;
  }

  const grouped = {};

  preview.forEach(row => {
    const itemId = row.item_id || "Unknown Item";
    const storeId = row.store_id || "Unknown Store";

    // group by item + store so same item from multiple stores does not combine
    const key = `${itemId}__${storeId}`;

    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(row);
  });

  let html = `<div class="prediction-card-grid">`;

  Object.entries(grouped).forEach(([key, rows]) => {
    const itemId = rows[0]?.item_id || "Unknown Item";
    const storeId = rows[0]?.store_id || "-";
    const category = rows[0]?.category || "-";
    const city = rows[0]?.city || "-";

    const avgSales =
      rows.reduce((sum, r) => sum + Number(r.predicted_sales || 0), 0) / rows.length;

    const maxSales = Math.max(...rows.map(r => Number(r.predicted_sales || 0)));

    const rainyDays = new Set(
      rows.filter(r => Number(r.rain_sum || 0) > 0).map(r => r.date)
    ).size;

    const forecastDays = new Set(rows.map(r => r.date)).size;
    const totalDays = rows.length;

    let overallSuggestion = "Stable demand — monitor inventory.";

    if (avgSales >= 25 || maxSales >= 35) {
      overallSuggestion = "🔥 High demand expected — restock aggressively.";
    } else if (avgSales >= 12 || maxSales >= 20) {
      overallSuggestion = "⚠️ Moderate demand expected — restock soon.";
    } else {
      overallSuggestion = "🧊 Low demand expected — no urgent restock needed.";
    }

    if (rainyDays > totalDays * 0.5) {
      overallSuggestion += " Rain may affect customer visits.";
    }

    html += `
      <div class="prediction-card">
        <div class="prediction-card-header">
          <div>
            <h4>${safeText(itemId)}</h4>
            <p>${safeText(category)} • ${safeText(city)} • ${safeText(storeId)}</p>
          </div>
          <span class="demand-pill ${getDemandClass(avgSales)}">
            Avg ${avgSales.toFixed(1)}
          </span>
        </div>

        <div class="prediction-stats">
          <div class="stat-box peak-sales">
            <span>Peak Sales</span>
            <strong>${maxSales.toFixed(1)}</strong>
          </div>

          <div>
            <span>Rainy Days</span>
            <strong>${rainyDays}</strong>
          </div>

          <div>
            <span>Forecast Days</span>
            <strong>${forecastDays}</strong>
          </div>
        </div>

        <div class="overall-suggestion">
          ${overallSuggestion}
        </div>
        <div class="mini-forecast-list">
          ${rows.slice(0, 5).map(r => `
            <div class="mini-forecast-row">
              <span>${safeText(r.date)}</span>
              <span>🌡️ ${safeText(r.temperature_2m_mean)}°C</span>
              <span>🌧️ ${safeText(r.rain_sum)} mm</span>
              <strong>${safeText(r.predicted_sales)}</strong>
            </div>
          `).join("")}
        </div>
      </div>
    `;
  });

  html += `</div>`;

  predictionTable.innerHTML = html;
}

document.addEventListener("DOMContentLoaded", () => {
  const payload = restorePredictionFromSession();

  console.log("Inventory page restored payload:", payload);

  if (!payload) {
    renderInventorySummary([]);
    renderPredictionTable([]);
    return;
  }

  const predictionRows =
    payload.full_results ||
    payload.results ||
    payload.predictions ||
    payload.preview ||
    [];

  console.log("Prediction rows used:", predictionRows);
  console.log("Unique items:", [...new Set(predictionRows.map(r => r.item_id))]);

  updateHeroMetricsFromPrediction(payload);
  renderInventorySummary(payload.inventory_summary || []);
  renderPredictionTable(predictionRows);
});