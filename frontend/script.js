const fetchBtn = document.getElementById("fetchBtn");
const cityInput = document.getElementById("cityInput");
const currentDiv = document.getElementById("currentWeather");
const forecastDiv = document.getElementById("forecast");

fetchBtn.addEventListener("click", () => {
    const city = cityInput.value.trim();
    if (!city) return alert("Please enter a city!");

    fetch(`http://127.0.0.1:5050/api/weather/${encodeURIComponent(city)}`)
        .then(res => res.json())
        .then(data => {
            if (data.error) {
                currentDiv.innerHTML = `<p>${data.error}</p>`;
                forecastDiv.innerHTML = "";
                return;
            }

            // Display current weather
            currentDiv.innerHTML = `
                <p>Temperature: ${data.current.temp} °C</p>
                <p>Feels like: ${data.current.feels_like} °C</p>
                <p>Humidity: ${data.current.humidity}%</p>
                <p>Wind speed: ${data.current.wind_speed} m/s</p>
                <p>Description: ${data.current.description}</p>
            `;

            // Display daily forecast
            forecastDiv.innerHTML = data.daily.map(d => `
                <p>${d.date}: ${d.avg_temp} °C, ${d.description}</p>
            `).join("");
        })
        .catch(err => {
            currentDiv.innerHTML = `<p>Error fetching data</p>`;
            forecastDiv.innerHTML = "";
            console.error(err);
        });
});