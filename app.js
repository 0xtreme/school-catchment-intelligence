const CATCHMENT_STATES = new Set(['NSW', 'VIC', 'QLD']);

const state = {
  dataset: null,
  map: null,
  selectedState: 'ALL',
  selectedSector: 'ALL',
  selectedType: 'ALL',
  selectedMetric: 'value_score',
  query: '',
  showCatchments: true,
  selectedSchoolId: null,
  filteredSchools: [],
  catchmentCache: new Map(),
  catchmentRequestSeq: 0,
};

const el = {
  catchmentStatus: document.getElementById('catchmentStatus'),
  catchmentProgress: document.getElementById('catchmentProgress'),
  stateSelect: document.getElementById('stateSelect'),
  sectorSelect: document.getElementById('sectorSelect'),
  typeSelect: document.getElementById('typeSelect'),
  metricSelect: document.getElementById('metricSelect'),
  searchInput: document.getElementById('searchInput'),
  showCatchments: document.getElementById('showCatchments'),
  metaLine: document.getElementById('metaLine'),
  coverageLine: document.getElementById('coverageLine'),
  selectedSchoolTitle: document.getElementById('selectedSchoolTitle'),
  selectedSchoolMeta: document.getElementById('selectedSchoolMeta'),
  selectedSchoolMetrics: document.getElementById('selectedSchoolMetrics'),
  topSchoolsChart: document.getElementById('topSchoolsChart'),
  tradeoffChart: document.getElementById('tradeoffChart'),
  hoverTip: document.getElementById('hoverTip'),
};

function setCatchmentStatus(message, pct = null) {
  if (el.catchmentStatus) {
    el.catchmentStatus.textContent = message;
  }

  if (el.catchmentProgress && Number.isFinite(pct)) {
    const safePct = Math.max(0, Math.min(100, pct));
    el.catchmentProgress.style.width = `${safePct}%`;
  }
}

function num(value, digits = 0) {
  if (!Number.isFinite(value)) {
    return 'N/A';
  }
  return value.toLocaleString('en-AU', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function pct(value, digits = 1) {
  if (!Number.isFinite(value)) {
    return 'N/A';
  }
  return `${value.toLocaleString('en-AU', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}%`;
}

function fetchJson(url) {
  return fetch(url).then((response) => {
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} for ${url}`);
    }
    return response.json();
  });
}

function buildSchoolFeatureCollection(rows) {
  return {
    type: 'FeatureCollection',
    features: rows
      .filter((row) => Number.isFinite(row.longitude) && Number.isFinite(row.latitude))
      .map((row) => ({
        type: 'Feature',
        id: row.acara_sml_id,
        geometry: {
          type: 'Point',
          coordinates: [row.longitude, row.latitude],
        },
        properties: {
          ...row,
        },
      })),
  };
}

function computeMetricStops(metric) {
  const values = state.filteredSchools
    .map((row) => Number(row[metric]))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);

  if (!values.length) {
    return { low: 0, mid: 50, high: 100 };
  }

  const low = values[Math.floor(values.length * 0.1)] ?? values[0];
  const mid = values[Math.floor(values.length * 0.5)] ?? values[Math.floor(values.length / 2)];
  const high = values[Math.floor(values.length * 0.9)] ?? values[values.length - 1];

  return { low, mid, high: Math.max(high, low + 1e-6) };
}

function metricExpression(metric) {
  const stops = computeMetricStops(metric);
  return [
    'case',
    ['!', ['has', metric]],
    '#b9c5d2',
    ['interpolate', ['linear'], ['to-number', ['get', metric]],
      stops.low, '#d6654f',
      stops.mid, '#f0c45e',
      stops.high, '#1a9f8c'],
  ];
}

function updateMapStyleByMetric() {
  if (!state.map) {
    return;
  }

  const metric = state.selectedMetric;
  const expression = metricExpression(metric);

  if (state.map.getLayer('schools-circles')) {
    state.map.setPaintProperty('schools-circles', 'circle-color', expression);
  }

  if (state.map.getLayer('catchments-fill')) {
    state.map.setPaintProperty('catchments-fill', 'fill-color', expression);
  }
}

function updateSchoolSource() {
  if (!state.map || !state.map.getSource('schools')) {
    return;
  }

  const geojson = buildSchoolFeatureCollection(state.filteredSchools);
  state.map.getSource('schools').setData(geojson);

  updateMapStyleByMetric();

  if (el.coverageLine) {
    el.coverageLine.textContent = `Schools shown: ${num(geojson.features.length)} across ${state.selectedState === 'ALL' ? 'Australia' : state.selectedState}`;
  }
}

async function loadCatchmentsForState(stateAbbr) {
  if (!CATCHMENT_STATES.has(stateAbbr)) {
    return { type: 'FeatureCollection', features: [] };
  }

  if (state.catchmentCache.has(stateAbbr)) {
    return state.catchmentCache.get(stateAbbr);
  }

  setCatchmentStatus(`Loading ${stateAbbr} official catchments...`, 15);
  const data = await fetchJson(`./data/catchments/${stateAbbr}.geojson`);
  state.catchmentCache.set(stateAbbr, data);
  setCatchmentStatus(`${stateAbbr} catchments loaded (${num(data.features?.length ?? 0)} polygons)`, 100);
  return data;
}

async function updateCatchmentSource() {
  if (!state.map || !state.map.getSource('catchments')) {
    return;
  }

  const requestId = ++state.catchmentRequestSeq;

  if (!state.showCatchments) {
    state.map.getSource('catchments').setData({ type: 'FeatureCollection', features: [] });
    setCatchmentStatus('Catchment layer hidden', 0);
    return;
  }

  if (state.selectedState === 'ALL') {
    state.map.getSource('catchments').setData({ type: 'FeatureCollection', features: [] });
    setCatchmentStatus('Select NSW, VIC, or QLD to load official catchment polygons', 0);
    return;
  }

  if (!CATCHMENT_STATES.has(state.selectedState)) {
    state.map.getSource('catchments').setData({ type: 'FeatureCollection', features: [] });
    setCatchmentStatus(`No official catchment dataset loaded for ${state.selectedState}`, 0);
    return;
  }

  try {
    const data = await loadCatchmentsForState(state.selectedState);
    if (requestId !== state.catchmentRequestSeq) {
      return;
    }

    state.map.getSource('catchments').setData(data);
    updateMapStyleByMetric();
  } catch (error) {
    if (requestId !== state.catchmentRequestSeq) {
      return;
    }

    state.map.getSource('catchments').setData({ type: 'FeatureCollection', features: [] });
    setCatchmentStatus(`Failed to load catchments: ${error.message}`, 0);
  }
}

function selectSchool(acaraId) {
  state.selectedSchoolId = acaraId;

  if (state.map && state.map.getLayer('schools-selected')) {
    state.map.setFilter('schools-selected', ['==', ['get', 'acara_sml_id'], acaraId ?? '']);
  }

  renderSelectedSchool();
}

function renderSelectedSchool() {
  const school = state.filteredSchools.find((item) => item.acara_sml_id === state.selectedSchoolId) ?? null;

  if (!school) {
    if (el.selectedSchoolTitle) {
      el.selectedSchoolTitle.textContent = 'Select a school';
    }
    if (el.selectedSchoolMeta) {
      el.selectedSchoolMeta.textContent = 'Click a school point to inspect value metrics.';
    }
    if (el.selectedSchoolMetrics) {
      el.selectedSchoolMetrics.innerHTML = '';
    }
    return;
  }

  el.selectedSchoolTitle.textContent = school.school_name;
  el.selectedSchoolMeta.textContent = `${school.school_sector} | ${school.school_type} | ${school.suburb}, ${school.state}`;

  const cards = [
    { label: 'Value score', value: num(school.value_score, 1) },
    { label: 'ICSEA', value: num(school.icsea, 0) },
    { label: 'Affordability percentile', value: pct(school.affordability_percentile, 1) },
    { label: 'Student-teacher ratio', value: num(school.student_teacher_ratio, 2) },
    { label: 'Median weekly rent (SA2)', value: school.median_weekly_rent ? `$${num(school.median_weekly_rent, 0)}` : 'N/A' },
    {
      label: 'Population growth 1Y (SA2)',
      value: Number.isFinite(school.population_growth_1y_pct)
        ? `${num(school.population_growth_1y_pct, 1)}%`
        : 'N/A',
    },
  ];

  el.selectedSchoolMetrics.innerHTML = cards
    .map(
      (card) => `
        <div class="metric-card">
          <div class="metric-label">${card.label}</div>
          <div class="metric-value">${card.value}</div>
        </div>
      `,
    )
    .join('');
}

function renderCharts() {
  const topRows = [...state.filteredSchools].sort((a, b) => b.value_score - a.value_score).slice(0, 16);

  Plotly.newPlot(
    el.topSchoolsChart,
    [
      {
        type: 'bar',
        orientation: 'h',
        y: topRows.map((row) => `${row.school_name} (${row.state})`),
        x: topRows.map((row) => row.value_score),
        marker: { color: '#0f9f9a' },
        hovertemplate: '%{y}<br>Value score: %{x:.1f}<extra></extra>',
      },
    ],
    {
      margin: { t: 6, l: 220, r: 14, b: 34 },
      yaxis: { autorange: 'reversed', color: '#406180' },
      xaxis: { title: 'Value score', color: '#406180' },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#294968', size: 11 },
    },
    { responsive: true, displaylogo: false, modeBarButtonsToRemove: ['lasso2d', 'select2d'] },
  );

  const scatterRows = state.filteredSchools.filter(
    (row) => Number.isFinite(row.icsea) && Number.isFinite(row.housing_burden) && Number.isFinite(row.value_score),
  );

  Plotly.newPlot(
    el.tradeoffChart,
    [
      {
        type: 'scattergl',
        mode: 'markers',
        x: scatterRows.map((row) => row.housing_burden * 100),
        y: scatterRows.map((row) => row.icsea),
        text: scatterRows.map(
          (row) => `${row.school_name}<br>${row.suburb}, ${row.state}<br>Value ${num(row.value_score, 1)}`,
        ),
        marker: {
          size: scatterRows.map((row) => Math.max(6, Math.min(22, Math.sqrt(row.total_enrolments || 0) / 6))),
          color: scatterRows.map((row) => row.value_score),
          colorscale: 'Viridis',
          opacity: 0.82,
          line: { width: 0.4, color: 'rgba(11,33,54,0.5)' },
        },
        hovertemplate: '%{text}<br>Housing burden %{x:.1f}%<br>ICSEA %{y:.0f}<extra></extra>',
      },
    ],
    {
      margin: { t: 6, l: 52, r: 14, b: 38 },
      xaxis: { title: 'SA2 housing burden (%)', color: '#406180' },
      yaxis: { title: 'ICSEA', color: '#406180' },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#294968', size: 11 },
    },
    { responsive: true, displaylogo: false, modeBarButtonsToRemove: ['lasso2d', 'select2d'] },
  );
}

function getFilteredSchools() {
  const query = state.query.trim().toLowerCase();

  return state.dataset.schools.filter((school) => {
    if (state.selectedState !== 'ALL' && school.state !== state.selectedState) {
      return false;
    }
    if (state.selectedSector !== 'ALL' && school.school_sector !== state.selectedSector) {
      return false;
    }
    if (state.selectedType !== 'ALL' && school.school_type !== state.selectedType) {
      return false;
    }

    if (!query) {
      return true;
    }

    return [school.school_name, school.suburb, school.sa2_name]
      .filter(Boolean)
      .some((value) => value.toLowerCase().includes(query));
  });
}

function applyFiltersAndRender() {
  state.filteredSchools = getFilteredSchools();
  updateSchoolSource();
  renderCharts();

  if (!state.filteredSchools.some((school) => school.acara_sml_id === state.selectedSchoolId)) {
    selectSchool(state.filteredSchools[0]?.acara_sml_id ?? null);
  } else {
    renderSelectedSchool();
  }

  updateCatchmentSource();
}

function initControls() {
  const states = [
    ['ALL', 'All Australia'],
    ...state.dataset.state_summary.map((row) => [row.state, `${row.state_name} (${row.state})`]),
  ];

  el.stateSelect.innerHTML = states
    .map(([code, label]) => `<option value="${code}">${label}</option>`)
    .join('');

  const sectors = ['ALL', ...new Set(state.dataset.schools.map((row) => row.school_sector).filter(Boolean))].sort();
  el.sectorSelect.innerHTML = sectors
    .map((sector) => `<option value="${sector}">${sector === 'ALL' ? 'All sectors' : sector}</option>`)
    .join('');

  const types = ['ALL', ...new Set(state.dataset.schools.map((row) => row.school_type).filter(Boolean))].sort();
  el.typeSelect.innerHTML = types
    .map((type) => `<option value="${type}">${type === 'ALL' ? 'All school types' : type}</option>`)
    .join('');

  el.stateSelect.addEventListener('change', (event) => {
    state.selectedState = event.target.value;
    applyFiltersAndRender();
  });

  el.sectorSelect.addEventListener('change', (event) => {
    state.selectedSector = event.target.value;
    applyFiltersAndRender();
  });

  el.typeSelect.addEventListener('change', (event) => {
    state.selectedType = event.target.value;
    applyFiltersAndRender();
  });

  el.metricSelect.addEventListener('change', (event) => {
    state.selectedMetric = event.target.value;
    updateMapStyleByMetric();
  });

  el.searchInput.addEventListener('input', (event) => {
    state.query = event.target.value;
    applyFiltersAndRender();
  });

  el.showCatchments.addEventListener('change', (event) => {
    state.showCatchments = event.target.checked;
    updateCatchmentSource();
  });
}

function initMap() {
  return new Promise((resolve) => {
    state.map = new maplibregl.Map({
      container: 'map',
      style: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
      center: [133.8, -25.3],
      zoom: 3.5,
      attributionControl: false,
    });

    state.map.addControl(new maplibregl.NavigationControl({ showCompass: true }), 'bottom-right');

    state.map.on('load', () => {
      state.map.addSource('catchments', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });

      state.map.addLayer({
        id: 'catchments-fill',
        type: 'fill',
        source: 'catchments',
        paint: {
          'fill-color': '#9fb0c2',
          'fill-opacity': 0.22,
        },
      });

      state.map.addLayer({
        id: 'catchments-line',
        type: 'line',
        source: 'catchments',
        paint: {
          'line-color': '#435e79',
          'line-width': 0.6,
          'line-opacity': 0.7,
        },
      });

      state.map.addSource('schools', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
        promoteId: 'acara_sml_id',
      });

      state.map.addLayer({
        id: 'schools-circles',
        type: 'circle',
        source: 'schools',
        paint: {
          'circle-color': '#0f9f9a',
          'circle-radius': [
            'interpolate',
            ['linear'],
            ['to-number', ['get', 'total_enrolments']],
            0,
            4,
            500,
            7,
            1500,
            11,
          ],
          'circle-stroke-color': '#ffffff',
          'circle-stroke-width': 0.8,
          'circle-opacity': 0.9,
        },
      });

      state.map.addLayer({
        id: 'schools-selected',
        type: 'circle',
        source: 'schools',
        filter: ['==', ['get', 'acara_sml_id'], ''],
        paint: {
          'circle-color': '#ffffff',
          'circle-stroke-color': '#173a5b',
          'circle-stroke-width': 2.1,
          'circle-radius': [
            'interpolate',
            ['linear'],
            ['to-number', ['get', 'total_enrolments']],
            0,
            6,
            500,
            9,
            1500,
            14,
          ],
          'circle-opacity': 0.95,
        },
      });

      state.map.on('mousemove', 'schools-circles', (event) => {
        const feature = event.features?.[0];
        if (!feature || !el.hoverTip) {
          return;
        }

        state.map.getCanvas().style.cursor = 'pointer';

        el.hoverTip.innerHTML = `
          <div class="title">${feature.properties.school_name}</div>
          <div class="line">${feature.properties.suburb}, ${feature.properties.state}</div>
          <div class="line">Value score: <strong>${num(Number(feature.properties.value_score), 1)}</strong></div>
          <div class="line">ICSEA: <strong>${num(Number(feature.properties.icsea), 0)}</strong></div>
        `;
        el.hoverTip.style.left = `${event.point.x}px`;
        el.hoverTip.style.top = `${event.point.y}px`;
        el.hoverTip.classList.remove('hidden');
      });

      state.map.on('mouseleave', 'schools-circles', () => {
        state.map.getCanvas().style.cursor = '';
        if (el.hoverTip) {
          el.hoverTip.classList.add('hidden');
        }
      });

      state.map.on('click', 'schools-circles', (event) => {
        const feature = event.features?.[0];
        if (!feature) {
          return;
        }
        selectSchool(feature.properties.acara_sml_id);
      });

      resolve();
    });
  });
}

function renderMeta() {
  const generatedAt = new Date(state.dataset.metadata.generated_at).toLocaleString('en-AU');
  el.metaLine.textContent =
    `Generated ${generatedAt} | ACARA profile ${state.dataset.metadata.school_profile_year} | ABS Census ${state.dataset.metadata.census_reference_year}`;
}

async function load() {
  try {
    setCatchmentStatus('Loading school dataset...', 6);
    state.dataset = await fetchJson('./data/school-catchment-dataset.json');

    renderMeta();
    initControls();

    setCatchmentStatus('Initializing map...', 24);
    await initMap();

    setCatchmentStatus('Map ready. Catchments load on selected state.', 0);
    applyFiltersAndRender();
  } catch (error) {
    setCatchmentStatus(`Startup failed: ${error.message}`, 0);
    // eslint-disable-next-line no-console
    console.error(error);
  }
}

load();
