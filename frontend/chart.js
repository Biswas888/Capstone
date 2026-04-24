let forecastChartInstance = null;
let revenueChartInstance = null;

function renderForecastChart(chartData) {
    const canvas = document.getElementById("forecastChart");
    if (!canvas) return;

    const ctx = canvas.getContext("2d");

    if (forecastChartInstance) {
        forecastChartInstance.destroy();
    }

    if (!chartData || chartData.length === 0) return;

    // Keep chart clean: show only first 15 points
    const cleanData = chartData.slice(0, 15);

    const labels = cleanData.map(row => row.date);
    const predictedSales = cleanData.map(row => Number(row.predicted_sales) || 0);
    const temperatures = cleanData.map(row =>
        row.temperature_2m_mean != null ? Number(row.temperature_2m_mean) : null
    );
    const rainData = cleanData.map(row =>
        row.rain_sum != null ? Number(row.rain_sum) : 0
    );

    forecastChartInstance = new Chart(ctx, {
        type: "line",
        data: {
            labels,
            datasets: [
                {
                    label: "Predicted Sales",
                    data: predictedSales,
                    borderWidth: 3,
                    tension: 0.4,
                    fill: false,
                    pointRadius: 0,
                    yAxisID: "y"
                },
                {
                    label: "Avg Temperature (°C)",
                    data: temperatures,
                    borderWidth: 2,
                    tension: 0.4,
                    borderDash: [6, 6],
                    fill: false,
                    pointRadius: 0,
                    yAxisID: "y1"
                },
                {
                    label: "Rain (mm)",
                    data: rainData,
                    type: "bar",
                    borderWidth: 1,
                    barPercentage: 0.45,
                    categoryPercentage: 0.55,
                    yAxisID: "y2"
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: "index",
                intersect: false
            },
            plugins: {
                legend: {
                    display: true,
                    position: "top",
                    labels: {
                        boxWidth: 14,
                        padding: 16
                    }
                },
                tooltip: {
                    enabled: true
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        autoSkip: true,
                        maxTicksLimit: 8
                    }
                },
                y: {
                    position: "left",
                    beginAtZero: true,
                    grid: {
                        color: "rgba(255,255,255,0.06)"
                    },
                    title: {
                        display: true,
                        text: "Predicted Sales"
                    }
                },
                y1: {
                    position: "right",
                    beginAtZero: false,
                    grid: {
                        drawOnChartArea: false
                    },
                    title: {
                        display: true,
                        text: "Temperature (°C)"
                    }
                },
                y2: {
                    position: "right",
                    beginAtZero: true,
                    display: false,
                    grid: {
                        drawOnChartArea: false
                    }
                }
            }
        }
    });
}

function renderRevenueChart(chartData) {
    const canvas = document.getElementById("revenueChart");
    if (!canvas) return;

    const ctx = canvas.getContext("2d");

    if (revenueChartInstance) {
        revenueChartInstance.destroy();
    }

    if (!chartData || chartData.length === 0) {
        console.log("No revenue chart data");
        return;
    }

    revenueChartInstance = new Chart(ctx, {
        type: "bar",
        data: {
            labels: chartData.map(row => row.date),
            datasets: [{
                label: "Revenue",
                data: chartData.map(row =>
                    Number(row.total_revenue ?? row.total_predicted_revenue ?? row.predicted_revenue ?? 0)
                )
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: "top"
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: "Revenue"
                    }
                }
            }
        }
    });
}