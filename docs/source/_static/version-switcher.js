document.addEventListener("DOMContentLoaded", function () {
  const select = document.getElementById("version-switcher");
  const currentVersion = window.location.pathname.split("/")[2];

  fetch("/_/versions/json/")
    .then(response => response.json())
    .then(data => {
      const versions = data.all;
      select.innerHTML = "";

      versions.forEach(v => {
        const option = document.createElement("option");
        option.value = v.slug;
        option.textContent = v.slug;
        if (v.slug === currentVersion) {
          option.selected = true;
        }
        select.appendChild(option);
      });
    });

  select.addEventListener("change", function () {
    const newVersion = select.value;
    const parts = window.location.pathname.split("/");
    parts[2] = newVersion;
    const newUrl = parts.join("/") + window.location.search + window.location.hash;
    window.location.href = newUrl;
  });
});
