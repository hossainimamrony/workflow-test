(function () {
  const base = '/workflows/real-footage-reels/api';
  const form = document.getElementById('run-form');
  const formStatus = document.getElementById('form-status');
  const runsList = document.getElementById('runs-list');
  const runsRefresh = document.getElementById('runs-refresh');
  const runsTrashDelete = document.getElementById('runs-trash-delete');
  const appMain = document.getElementById('app-main');
  const pageTitle = document.getElementById('page-title');
  const navStudio = document.getElementById('nav-studio');
  const navRuns = document.getElementById('nav-runs');
  const studioPanel = document.getElementById('studio-panel');
  const runsPanel = document.getElementById('runs-panel');
  const runDetailPanel = document.getElementById('run-detail-panel');
  const invQ = document.getElementById('inventory-q');
  const invRefresh = document.getElementById('inventory-refresh');
  const invResults = document.getElementById('inventory-results');
  const invNote = document.getElementById('inventory-note');
  const invClear = document.getElementById('inventory-clear');
  const currentJobPanel = document.getElementById('current-job-panel');
  const currentJobStatus = document.getElementById('current-job-status');
  const currentJobMeta = document.getElementById('current-job-meta');
  const currentJobList = document.getElementById('current-job-list');
  let latestInventoryMatches = [];
  const routePath = appMain ? String(appMain.dataset.routePath || window.location.pathname || '') : String(window.location.pathname || '');
  const routeRunId = appMain ? String(appMain.dataset.runId || '') : '';
  const runRouteMatch = /\/workflows\/real-footage-reels\/runs\/([^/]+)$/u.exec(routePath);
  const currentRunId = routeRunId || (runRouteMatch ? decodeURIComponent(runRouteMatch[1]) : '');
  const appBase = '/workflows/real-footage-reels';

  function getCookie(name) {
    const parts = document.cookie ? document.cookie.split(';') : [];
    for (const part of parts) {
      const [k, ...rest] = part.trim().split('=');
      if (k === name) return decodeURIComponent(rest.join('='));
    }
    return '';
  }

  async function api(url, opts) {
    const req = opts ? { ...opts } : {};
    req.headers = req.headers ? { ...req.headers } : {};
    if (req.method && req.method.toUpperCase() !== 'GET') {
      const csrf = getCookie('csrftoken');
      if (csrf && !req.headers['X-CSRFToken']) req.headers['X-CSRFToken'] = csrf;
    }

    const r = await fetch(url, req);
    const raw = await r.text();
    const contentType = String(r.headers.get('content-type') || '').toLowerCase();
    let data;
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_err) {
      if (!r.ok) {
        const cleaned = String(raw || '')
          .replace(/<[^>]*>/g, ' ')
          .replace(/\s+/g, ' ')
          .trim()
          .slice(0, 400);
        throw new Error(cleaned || `Request failed (${r.status}).`);
      }
      throw new Error('Server returned non-JSON response.');
    }

    if (!r.ok) {
      const apiError = data && typeof data.error === 'string' ? data.error.trim() : '';
      const fallback = !contentType.includes('application/json')
        ? String(raw || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 400)
        : '';
      throw new Error(apiError || fallback || `Request failed (${r.status}).`);
    }
    return data;
  }

  async function deleteRunById(runId) {
    const encodedRunId = encodeURIComponent(String(runId || '').trim());
    if (!encodedRunId) {
      throw new Error('Missing run id.');
    }
    try {
      return await api(base + '/runs/' + encodedRunId, { method: 'DELETE' });
    } catch (_err) {
      return api(base + '/runs/' + encodedRunId + '/delete', { method: 'POST' });
    }
  }

  function esc(v) { return String(v || '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
  function fmtDate(v) {
    if (!v) return 'Unknown';
    const d = new Date(v);
    if (!Number.isFinite(d.getTime())) return 'Unknown';
    return d.toLocaleString();
  }
  function formatMediaTime(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return '0:00';
    const totalSeconds = Math.floor(n);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (hours > 0) return `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    return `${minutes}:${String(seconds).padStart(2, '0')}`;
  }
  function buildVideoDownloadName(src, title) {
    const safeTitle = String(title || 'video')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'video';
    let extension = 'mp4';
    try {
      const parsed = new URL(String(src || ''), window.location.origin);
      const filePath = parsed.searchParams.get('path') || parsed.pathname;
      const match = /\.([a-z0-9]{2,5})(?:$|\?)/iu.exec(filePath);
      if (match) extension = String(match[1] || '').toLowerCase();
    } catch (_err) {
      extension = 'mp4';
    }
    return `${safeTitle}.${extension}`;
  }
  function buildImageDownloadName(title, stockId, runId) {
    const safeBase = String(title || stockId || runId || 'thumbnail')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'thumbnail';
    return `${safeBase}.png`;
  }
  function withVersionToken(url, token) {
    const value = String(url || '').trim();
    if (!value) return '';
    try {
      const parsed = new URL(value, window.location.origin);
      parsed.searchParams.set('v', String(token));
      return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    } catch (_err) {
      return value;
    }
  }
  function fileToDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ''));
      reader.onerror = () => reject(new Error('Could not read the selected image.'));
      reader.readAsDataURL(file);
    });
  }
  async function startDirectDownload(url, fileName) {
    const targetUrl = String(url || '').trim();
    const targetName = String(fileName || 'video.mp4').trim() || 'video.mp4';
    if (!targetUrl) return;
    try {
      const resp = await fetch(targetUrl, { method: 'GET' });
      if (!resp.ok) throw new Error(`Download failed (${resp.status})`);
      const blob = await resp.blob();
      const blobUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = blobUrl;
      link.download = targetName;
      link.style.display = 'none';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      setTimeout(() => URL.revokeObjectURL(blobUrl), 2000);
      return;
    } catch (_err) {
      // Fallback if CORS/fetch is blocked: still trigger browser navigation.
    }
    const fallback = document.createElement('a');
    fallback.href = targetUrl;
    fallback.download = targetName;
    fallback.style.display = 'none';
    document.body.appendChild(fallback);
    fallback.click();
    document.body.removeChild(fallback);
  }
  function renderVideoPlayer(src, title, opts) {
    const options = opts || {};
    const compact = Boolean(options.compact);
    const extraClass = String(options.className || '').trim();
    const classes = ['video-player'];
    if (compact) classes.push('video-player--compact');
    if (extraClass) classes.push(extraClass);
    const playerClassName = classes.join(' ');
    const downloadSrc = String(options.downloadSrc || src || '');
    const downloadName = buildVideoDownloadName(downloadSrc, title);
    return `
      <div class="${esc(playerClassName)}" data-video-player>
        <div class="video-player__stage">
          <video class="video-player__media" preload="metadata" playsinline src="${esc(src || '')}"></video>
          <div class="video-player__status" data-role="status" hidden></div>
        </div>
        <div class="video-player__controls">
          <div class="video-player__row">
            <button type="button" class="video-player__button" data-role="play">Play</button>
            <button type="button" class="video-player__button" data-role="restart">Restart</button>
            <a class="video-player__button video-player__button--link" data-role="download" href="${esc(downloadSrc)}" download="${esc(downloadName)}" data-download-name="${esc(downloadName)}">Download</a>
            <span class="video-player__time" data-role="time">0:00 / 0:00</span>
            <button type="button" class="video-player__button" data-role="mute">Mute</button>
            ${compact ? '' : `<input class="video-player__volume" data-role="volume" type="range" min="0" max="1" step="0.05" value="1" aria-label="Volume for ${esc(title || 'Video')}">`}
            <button type="button" class="video-player__button" data-role="fullscreen">Full</button>
          </div>
          <input class="video-player__timeline" data-role="timeline" type="range" min="0" max="0.1" step="0.1" value="0" aria-label="Timeline for ${esc(title || 'Video')}">
        </div>
      </div>
    `;
  }
  function cleanupVideoPlayers(root) {
    if (!root) return;
    root.querySelectorAll('[data-video-player]').forEach((player) => {
      const cleanup = player._cleanupVideoPlayer;
      if (typeof cleanup === 'function') cleanup();
    });
  }
  function initVideoPlayers(root) {
    if (!root) return;
    root.querySelectorAll('[data-video-player]').forEach((player) => {
      if (player.dataset.playerBound === '1') return;
      player.dataset.playerBound = '1';

      const video = player.querySelector('.video-player__media');
      const status = player.querySelector('[data-role="status"]');
      const playBtn = player.querySelector('[data-role="play"]');
      const restartBtn = player.querySelector('[data-role="restart"]');
      const muteBtn = player.querySelector('[data-role="mute"]');
      const fullscreenBtn = player.querySelector('[data-role="fullscreen"]');
      const downloadLink = player.querySelector('[data-role="download"]');
      const volumeInput = player.querySelector('[data-role="volume"]');
      const timelineInput = player.querySelector('[data-role="timeline"]');
      const timeEl = player.querySelector('[data-role="time"]');
      if (!video || !playBtn || !restartBtn || !muteBtn || !fullscreenBtn || !timelineInput || !timeEl) return;

      let waiting = Boolean(video.getAttribute('src'));
      let errorText = '';
      let isScrubbing = false;

      function setStatus(message, toneError) {
        if (!status) return;
        const text = String(message || '').trim();
        status.hidden = !text;
        status.textContent = text;
        status.classList.toggle('video-player__status--error', Boolean(toneError));
      }

      function syncState() {
        const duration = Number.isFinite(video.duration) ? video.duration : 0;
        const currentTime = Number.isFinite(video.currentTime) ? video.currentTime : 0;
        const volume = Number.isFinite(video.volume) ? video.volume : 1;
        const muted = Boolean(video.muted);
        const playing = !video.paused && !video.ended;
        const fullscreen = document.fullscreenElement === player;

        playBtn.textContent = playing ? 'Pause' : 'Play';
        muteBtn.textContent = (muted || volume <= 0) ? 'Unmute' : 'Mute';
        fullscreenBtn.textContent = fullscreen ? 'Exit Full' : 'Full';
        timeEl.textContent = `${formatMediaTime(currentTime)} / ${formatMediaTime(duration)}`;
        const timelineMax = Math.max(duration, 0.1);
        timelineInput.max = String(timelineMax);
        if (!isScrubbing) {
          timelineInput.value = String(Math.min(currentTime, timelineMax));
        }
        if (volumeInput) {
          volumeInput.value = String(muted ? 0 : volume);
        }
        if (errorText) {
          setStatus(errorText, true);
        } else if (waiting) {
          setStatus('Loading...', false);
        } else {
          setStatus('', false);
        }
      }

      function handleLoaded() {
        waiting = false;
        errorText = '';
        syncState();
      }

      function handleWaiting() {
        waiting = true;
        syncState();
      }

      function handlePlaying() {
        waiting = false;
        syncState();
      }

      function handleError() {
        waiting = false;
        errorText = 'Unable to load video.';
        syncState();
      }

      async function handleTogglePlay() {
        if (video.paused) {
          try {
            await video.play();
          } catch (_err) {
            errorText = 'Playback was blocked.';
            waiting = false;
            syncState();
          }
          return;
        }
        video.pause();
      }

      function handleSeek() {
        const nextTime = Number(timelineInput.value || 0);
        video.currentTime = Number.isFinite(nextTime) ? nextTime : 0;
        syncState();
      }
      function startScrub() {
        isScrubbing = true;
      }
      function endScrub() {
        if (!isScrubbing) return;
        isScrubbing = false;
        handleSeek();
      }

      function handleVolumeChange() {
        if (!volumeInput) return;
        const nextVolume = Number(volumeInput.value || 0);
        video.volume = Number.isFinite(nextVolume) ? nextVolume : 0;
        video.muted = video.volume <= 0;
        syncState();
      }

      function handleToggleMute() {
        const nextMuted = !video.muted;
        video.muted = nextMuted;
        if (!nextMuted && video.volume <= 0) video.volume = 1;
        syncState();
      }

      function handleRestart() {
        video.currentTime = 0;
        syncState();
      }

      async function handleToggleFullscreen() {
        try {
          if (document.fullscreenElement === player) {
            await document.exitFullscreen();
            return;
          }
          await player.requestFullscreen();
        } catch (_err) {
          errorText = 'Fullscreen unavailable.';
          waiting = false;
          syncState();
        }
      }

      const onTogglePlayClick = () => { void handleTogglePlay(); };
      const onToggleFullscreenClick = () => { void handleToggleFullscreen(); };
      const onVideoDoubleClick = () => { void handleToggleFullscreen(); };
      const onDownloadClick = (event) => {
        if (event && typeof event.preventDefault === 'function') event.preventDefault();
        const src = String((downloadLink && downloadLink.getAttribute('href')) || video.currentSrc || video.src || '').trim();
        const name = String((downloadLink && downloadLink.getAttribute('data-download-name')) || buildVideoDownloadName(src, '')).trim();
        void startDirectDownload(src, name);
      };

      playBtn.addEventListener('click', onTogglePlayClick);
      restartBtn.addEventListener('click', handleRestart);
      muteBtn.addEventListener('click', handleToggleMute);
      fullscreenBtn.addEventListener('click', onToggleFullscreenClick);
      if (downloadLink) downloadLink.addEventListener('click', onDownloadClick);
      timelineInput.addEventListener('input', handleSeek);
      timelineInput.addEventListener('change', endScrub);
      timelineInput.addEventListener('pointerdown', startScrub);
      timelineInput.addEventListener('pointerup', endScrub);
      timelineInput.addEventListener('pointercancel', endScrub);
      window.addEventListener('pointerup', endScrub);
      window.addEventListener('blur', endScrub);
      if (volumeInput) volumeInput.addEventListener('input', handleVolumeChange);
      video.addEventListener('loadedmetadata', handleLoaded);
      video.addEventListener('loadeddata', handleLoaded);
      video.addEventListener('durationchange', syncState);
      video.addEventListener('timeupdate', syncState);
      video.addEventListener('volumechange', syncState);
      video.addEventListener('play', handlePlaying);
      video.addEventListener('pause', syncState);
      video.addEventListener('ended', syncState);
      video.addEventListener('waiting', handleWaiting);
      video.addEventListener('playing', handlePlaying);
      video.addEventListener('canplay', handleLoaded);
      video.addEventListener('error', handleError);
      video.addEventListener('dblclick', onVideoDoubleClick);
      document.addEventListener('fullscreenchange', syncState);

      syncState();

      player._cleanupVideoPlayer = () => {
        playBtn.removeEventListener('click', onTogglePlayClick);
        restartBtn.removeEventListener('click', handleRestart);
        muteBtn.removeEventListener('click', handleToggleMute);
        fullscreenBtn.removeEventListener('click', onToggleFullscreenClick);
        if (downloadLink) downloadLink.removeEventListener('click', onDownloadClick);
        timelineInput.removeEventListener('input', handleSeek);
        timelineInput.removeEventListener('change', endScrub);
        timelineInput.removeEventListener('pointerdown', startScrub);
        timelineInput.removeEventListener('pointerup', endScrub);
        timelineInput.removeEventListener('pointercancel', endScrub);
        window.removeEventListener('pointerup', endScrub);
        window.removeEventListener('blur', endScrub);
        if (volumeInput) volumeInput.removeEventListener('input', handleVolumeChange);
        video.removeEventListener('loadedmetadata', handleLoaded);
        video.removeEventListener('loadeddata', handleLoaded);
        video.removeEventListener('durationchange', syncState);
        video.removeEventListener('timeupdate', syncState);
        video.removeEventListener('volumechange', syncState);
        video.removeEventListener('play', handlePlaying);
        video.removeEventListener('pause', syncState);
        video.removeEventListener('ended', syncState);
        video.removeEventListener('waiting', handleWaiting);
        video.removeEventListener('playing', handlePlaying);
        video.removeEventListener('canplay', handleLoaded);
        video.removeEventListener('error', handleError);
        video.removeEventListener('dblclick', onVideoDoubleClick);
        document.removeEventListener('fullscreenchange', syncState);
      };
    });
  }
  function setRunModeUi() {
    if (pageTitle) pageTitle.textContent = 'Run';
    if (navStudio) navStudio.classList.remove('is-active');
    if (navRuns) navRuns.classList.add('is-active');
  }
  function setStudioModeUi() {
    if (pageTitle) pageTitle.textContent = 'Studio';
    if (navRuns) navRuns.classList.remove('is-active');
    if (navStudio) navStudio.classList.add('is-active');
  }
  function commandLabel(command) {
    if (command === 'download') return 'Download';
    if (command === 'prepare') return 'Prepare';
    if (command === 'script-draft') return 'Script Prep';
    if (command === 'run') return 'Script Prep';
    if (command === 'compose') return 'Compose';
    return String(command || 'Job');
  }
  function renderCurrentJob(jobs, maxParallelJobs, workerMode) {
    if (!currentJobPanel) return;
    const activeJobs = Array.isArray(jobs) ? jobs.filter((job) => job && (job.status === 'queued' || job.status === 'running' || job.status === 'paused')) : [];
    if (!activeJobs.length) {
      currentJobPanel.style.display = 'none';
      return;
    }
    currentJobPanel.style.display = 'grid';
    if (currentJobStatus) {
      const runningCount = activeJobs.filter((job) => job.status === 'running').length;
      currentJobStatus.textContent = `${runningCount} running`;
      currentJobStatus.className = 'job-summary__status job-summary__status--running';
    }
    if (currentJobMeta) {
      const queuedCount = activeJobs.filter((job) => job.status === 'queued').length;
      const pausedCount = activeJobs.filter((job) => job.status === 'paused').length;
      const cap = Number(maxParallelJobs || 0);
      const mode = String(workerMode || '').trim();
      const modeText = mode ? ` | worker ${mode}` : '';
      currentJobMeta.textContent = `${activeJobs.length} active | ${queuedCount} queued | ${pausedCount} paused${cap > 0 ? ` | capacity ${cap}` : ''}${modeText}`;
    }
    if (currentJobList) {
      currentJobList.innerHTML = activeJobs.slice(0, 20).map((job) => {
        const progress = job.progress || {};
        const pct = Math.max(0, Math.min(100, Number(progress.percent || 0)));
        const src = [job.listingTitle || '', job.stockId || '', (job.urls && job.urls[0]) || ''].filter(Boolean).join(' | ');
        const runId = String(job.runId || (job.result && job.result.runId) || '').trim();
        const canPause = job.status === 'queued';
        const canResume = job.status === 'paused';
        const canStop = job.status === 'queued' || job.status === 'running' || job.status === 'paused';
        const canRemove = job.status !== 'running';
        return `<article class="run-row">
          <div class="run-row__main">
            <div class="run-row__header">
              <strong class="run-row__title">${esc(commandLabel(job.command))}</strong>
              <span class="job-summary__status job-summary__status--${esc(String(job.status || '').toLowerCase())}">${esc(String(job.status || 'queued'))}</span>
            </div>
            <div class="run-row__meta">${esc(src || '-')}</div>
            <div class="progress-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}">
              <div class="progress-bar__fill" style="width:${pct}%"></div>
            </div>
            <div class="run-row__stats"><span>${esc(String(progress.phase || 'queued'))}</span><span>${esc(String(progress.label || 'Waiting in queue...'))}</span></div>
            <div class="run-row__actions">
              ${runId ? `<a class="button button--secondary" href="${esc(`${appBase}/runs/${encodeURIComponent(runId)}`)}">View</a>` : ''}
              <button class="button button--ghost" type="button" data-job-action="pause" data-job-id="${esc(String(job.id || ''))}" ${canPause ? '' : 'disabled'}>Pause</button>
              <button class="button button--ghost" type="button" data-job-action="resume" data-job-id="${esc(String(job.id || ''))}" ${canResume ? '' : 'disabled'}>Resume</button>
              <button class="button button--danger" type="button" data-job-action="stop" data-job-id="${esc(String(job.id || ''))}" ${canStop ? '' : 'disabled'}>Stop</button>
              <button class="button button--secondary" type="button" data-job-action="remove" data-job-id="${esc(String(job.id || ''))}" ${canRemove ? '' : 'disabled'}>Remove</button>
            </div>
          </div>
        </article>`;
      }).join('');
      currentJobList.querySelectorAll('[data-job-action]').forEach((button) => {
        button.addEventListener('click', async () => {
          const jobId = String(button.getAttribute('data-job-id') || '').trim();
          const action = String(button.getAttribute('data-job-action') || '').trim();
          if (!jobId || !action) return;
          button.disabled = true;
          try {
            await api(base + '/jobs/' + encodeURIComponent(jobId) + '/control', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ action }),
            });
            await loadCurrentJob();
            await loadRuns();
          } catch (err) {
            alert(err.message || String(err));
          } finally {
            button.disabled = false;
          }
        });
      });
    }
  }
  async function loadCurrentJob() {
    try {
      const payload = await api(base + '/jobs');
      const jobs = Array.isArray(payload.jobs) ? payload.jobs : [];
      renderCurrentJob(jobs, payload.maxParallelJobs, payload.workerMode);
      return jobs;
    } catch (_err) {
      renderCurrentJob([], 0, '');
      return [];
    }
  }

  async function loadRuns() {
    const [runsData, jobsData] = await Promise.all([
      api(base + '/runs'),
      api(base + '/jobs').catch(() => ({ jobs: [] })),
    ]);
    const runs = runsData.runs || [];
    const jobs = Array.isArray(jobsData.jobs) ? jobsData.jobs : [];
    const latestJobsByRunId = new Map();
    jobs.forEach((job) => {
      if (!job) return;
      const runId = String(job.runId || '').trim();
      if (!runId) return;
      if (!latestJobsByRunId.has(runId)) latestJobsByRunId.set(runId, job);
    });
    runsList.innerHTML = runs.length ? runs.map((run) => runRow(run, latestJobsByRunId.get(String(run.runId || '')))).join('') : '<div class="empty-block"><strong>No runs</strong></div>';
    runsList.querySelectorAll('[data-view]').forEach((b) => b.onclick = () => {
      const runId = b.dataset.view;
      if (!runId) return;
      window.location.href = `/workflows/real-footage-reels/runs/${encodeURIComponent(runId)}`;
    });
    runsList.querySelectorAll('[data-del]').forEach((b) => b.onclick = async () => {
      if (!confirm('Delete this run?')) return;
      await deleteRunById(b.dataset.del);
      await loadRuns();
    });
  }

  async function deleteTrash() {
    const confirmed = window.confirm('Delete all raw footage (.mov/.mp4) from runs and keep only final-reel.mp4 files?');
    if (!confirmed) return;
    if (runsTrashDelete) runsTrashDelete.disabled = true;
    try {
      let result;
      try {
        result = await api(`${base}/runs/trash-cleanup`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ confirmed: true }),
        });
      } catch (_err) {
        // Fallback for environments where POST/CSRF handling is inconsistent.
        result = await api(`${base}/runs/trash-cleanup?confirmed=1`, { method: 'GET' });
      }
      const deletedFiles = Number(result.deletedFiles || 0);
      const deletedDirs = Number(result.deletedDirs || 0);
      const deletedBytes = Number(result.deletedBytes || 0);
      const deletedMb = (deletedBytes / (1024 * 1024)).toFixed(1);
      window.alert(`Trash cleaned.\nDeleted files: ${deletedFiles}\nDeleted folders: ${deletedDirs}\nFreed: ${deletedMb} MB`);
      await loadRuns();
    } catch (err) {
      window.alert(err.message || 'Trash cleanup failed.');
    } finally {
      if (runsTrashDelete) runsTrashDelete.disabled = false;
    }
  }

  async function loadRunDetail(runId) {
    if (!runId) return;
    setRunModeUi();
    if (studioPanel) studioPanel.style.display = 'none';
    if (runsPanel) runsPanel.style.display = 'none';
    cleanupVideoPlayers(runDetailPanel);
    if (runDetailPanel) runDetailPanel.style.display = 'block';
    if (runDetailPanel) runDetailPanel.innerHTML = '<div class="empty-block"><strong>Loading run...</strong></div>';
    const run = await api(base + '/runs/' + encodeURIComponent(runId));
    const p = run.pipeline || {};
    const stats = run.stats || {};
    const videos = Array.isArray(run.videos) ? run.videos : [];
    const previewClips = videos.filter(v => v && v.videoUrl).slice(0, 4);
    const sequenceItems = Array.isArray(run?.plan?.composition?.segments) && run.plan.composition.segments.length
      ? run.plan.composition.segments
      : (Array.isArray(run?.plan?.sequence) ? run.plan.sequence : []);
    const heroVideoUrl = run.finalReelPreviewUrl || run.finalReelUrl || run.mainReelUrl || (previewClips[0] ? previewClips[0].videoUrl : '');
    const heroDownloadUrl = run.finalReelUrl || run.finalReelPreviewUrl || run.mainReelUrl || heroVideoUrl;
    const scriptVariants = Array.isArray(run?.voiceoverDraft?.variants) ? run.voiceoverDraft.variants : [];
    const analysisReady = Boolean(run?.pipeline?.analyze?.done);
    const priceIncludes = String(run.priceIncludes || (run.report && run.report.priceIncludes) || '').trim();
    const autoPrice = String(run.listingPrice || '').trim() || 'AU ';
    const remotePublishError = String(run.finalReelRemoteError || '').trim();
    const remotePublishFailed = run.finalReelRemoteUploadOk === false && Boolean(remotePublishError);

    if (!runDetailPanel) return;
    runDetailPanel.innerHTML = `
      <div class="page-stack">
        <section class="panel detail-toolbar detail-toolbar--compact">
          <div class="detail-toolbar__copy">
            <h3 class="dashboard__panel-title">${esc(run.listingTitle || run.runId || 'Run')}</h3>
            <p class="detail-toolbar__meta">${esc(run.stockId ? `${run.stockId} | ` : '')}${esc(fmtDate(run.createdAt))}</p>
          </div>
          <div class="detail-actions">
            <button class="button button--secondary" type="button" id="back-studio">Back</button>
            <button class="button button--danger" type="button" id="delete-run">Delete</button>
          </div>
        </section>

        <section class="panel detail-overview">
          <div class="detail-overview__media">
            <p class="detail-overview__label">${heroVideoUrl ? (run.finalReelPreviewUrl ? 'Final Reel (Preview Stream)' : (run.finalReelUrl ? 'Final Reel' : (run.mainReelUrl ? 'Main Reel (building final)' : 'Preview'))) : 'Preview'}</p>
            ${heroVideoUrl ? `<div class="preview-frame preview-frame--hero">${renderVideoPlayer(heroVideoUrl, run.listingTitle || run.runId || 'Video', { compact: false, downloadSrc: heroDownloadUrl })}</div>` : '<div class="empty-block"><strong>No preview</strong></div>'}
          </div>

          <div class="detail-overview__sidebar">
            <div class="detail-overview__header">
              <div>
                <p class="run-card__eyebrow">${esc(run.stockId ? `Stock ${run.stockId}` : 'Run')}</p>
                <h3>${esc(run.listingTitle || run.runId || 'Run')}</h3>
                <p class="detail-run-id">${esc(fmtDate(run.createdAt))}</p>
              </div>
              <div class="pipeline-dots">
                <span class="pipeline-dot ${p.download && p.download.done ? 'pipeline-dot--on' : ''}"></span>
                <span class="pipeline-dot ${p.frames && p.frames.done ? 'pipeline-dot--on' : ''}"></span>
                <span class="pipeline-dot ${p.analyze && p.analyze.done ? 'pipeline-dot--on' : ''}"></span>
                <span class="pipeline-dot ${p.render && p.render.done ? 'pipeline-dot--on' : ''}"></span>
              </div>
            </div>

            <dl class="detail-summary-grid">
              <div class="metric"><dt>Clips</dt><dd>${Number(stats.downloads || 0)}</dd></div>
              <div class="metric"><dt>Frames</dt><dd>${Number(stats.frames || 0)}</dd></div>
              <div class="metric"><dt>AI</dt><dd>${Number(stats.analyzed || 0)}</dd></div>
              <div class="metric"><dt>Cut</dt><dd>${Number(stats.planned || 0)}</dd></div>
            </dl>

            ${run.carDescription ? `<details class="run-detail__description" open><summary>Description</summary><p class="run-detail__description-body">${esc(run.carDescription)}</p></details>` : ''}
            ${priceIncludes ? `<details class="run-detail__description" open><summary>Price Includes</summary><p class="run-detail__description-body">${esc(priceIncludes)}</p></details>` : ''}
            ${run.hasVoiceover && run.voiceoverScript ? `<details class="run-detail__voiceover" open><summary>Voice-over</summary><p class="run-detail__voiceover-body">${esc(run.voiceoverScript)}</p></details>` : ''}
            ${remotePublishFailed ? `<div class="callout callout--warning"><div class="callout__copy"><strong>Remote download unavailable</strong><p>${esc(remotePublishError)}</p></div></div>` : ''}
          </div>
        </section>

        <section class="panel thumbnail-generator">
          <div class="section-heading section-heading--compact"><h3 class="section-heading__title section-heading__title--panel">Thumbnail Generator</h3></div>
          <div class="thumbnail-generator__grid">
            <form id="thumbnail-form" class="thumbnail-generator__form">
              <input id="thumbnail-reference-input" type="file" accept="image/*" tabindex="-1" class="thumbnail-generator__file-input">
              <label class="field"><span class="field__label">Title</span><input id="thumbnail-title" class="field__input" value="${esc(run.listingTitle || '')}"></label>
              <label class="field"><span class="field__label">Subtitle</span><input id="thumbnail-subtitle" class="field__input" value="" placeholder="2.8L Turbo Diesel | 4WD"></label>
              <label class="field"><span class="field__label">Price (Auto)</span><input id="thumbnail-price" class="field__input" value="${esc(autoPrice)}"></label>
              <div id="thumbnail-error-wrap"></div>
              <div class="form-actions"><button id="btn-generate-thumbnail" class="button button--primary" type="submit">Generate Thumbnail</button></div>
            </form>
            <div id="thumbnail-preview-card" class="thumbnail-generator__preview-card">
              <button type="button" id="thumbnail-preview-pick" class="thumbnail-generator__preview-stage thumbnail-generator__preview-stage--pick">
                <div class="thumbnail-generator__placeholder"><strong>Choose reference image</strong><span>Click here to select the car photo.</span></div>
              </button>
              <div class="thumbnail-generator__preview-floating-actions">
                <a id="thumbnail-download-link" class="button button--secondary" href="#" download="${esc(buildImageDownloadName(run.listingTitle, run.stockId, run.runId))}" style="display:none">Download Image</a>
                <button id="thumbnail-choose-image" class="button button--ghost" type="button">Choose New Image</button>
              </div>
            </div>
          </div>
        </section>

        <section class="panel voiceover-script-panel">
          <h3 class="section-heading__title section-heading__title--panel">Video Script</h3>
          ${scriptVariants.length ? `
            <div class="voiceover-script-panel__variants">
              ${scriptVariants.map((v, i) => `<label class="voiceover-variant"><input type="radio" name="script-variant" data-script="${esc(v.script || '')}" ${i === 0 ? 'checked' : ''}><span class="voiceover-variant__label">${esc(v.label || v.id || 'Option')}</span><span class="voiceover-variant__preview">${esc(v.script || '')}</span></label>`).join('')}
            </div>
            <label class="field"><span class="field__label">Script to stitch</span><textarea id="script-to-stitch" class="field__input field__input--textarea" rows="4">${esc((scriptVariants[0] && scriptVariants[0].script) || run.voiceoverScript || '')}</textarea></label>
          ` : '<div class="empty-block"><strong>No script options yet</strong></div>'}
          <div class="voiceover-script-panel__row">
            <button id="btn-regenerate-scripts" class="button button--secondary" type="button">Regenerate options</button>
            <button id="btn-generate-full" class="button button--primary" type="button">${analysisReady ? 'Compose Final Video' : 'Generate Full Video'}</button>
          </div>
          <p class="field__hint">${analysisReady ? 'Step 2: compose and stitch voice-over.' : 'Step 2: after script approval, the system auto-runs prepare + compose (no second click needed).'}</p>
        </section>

        <section class="panel">
          <div class="section-heading section-heading--compact"><h3 class="section-heading__title">Sequence</h3></div>
          ${sequenceItems.length ? `<div class="sequence-grid">${sequenceItems.map((item, idx) => `
            <article class="sequence-card">
              <div class="sequence-card__header"><span class="role-pill">${esc((item.purpose || item.role || 'clip').replace(/_/g, ' '))}</span><span class="sequence-card__label">${esc(((item.analysis && item.analysis.primaryLabel) || item.primaryLabel || 'Unclassified').replace(/_/g, ' '))}</span></div>
              <h4>${esc(item.title || item.clipId || `Clip ${idx + 1}`)}</h4>
              ${item.frameUrl ? `<a class="sequence-card__frame" href="${esc(item.frameUrl)}" target="_blank" rel="noreferrer"><img src="${esc(item.frameUrl)}" alt=""></a>` : '<div class="sequence-card__frame sequence-card__frame--empty"><span>-</span></div>'}
              <div class="sequence-card__actions">
                ${item.videoUrl ? `<a class="button button--ghost" href="${esc(item.videoUrl)}" target="_blank" rel="noreferrer">Video</a>` : ''}
                ${item.frameUrl ? `<a class="button button--secondary" href="${esc(item.frameUrl)}" target="_blank" rel="noreferrer">Frame</a>` : ''}
              </div>
            </article>`).join('')}</div>` : '<div class="empty-block"><strong>No sequence</strong></div>'}
        </section>
      </div>
    `;
    initVideoPlayers(runDetailPanel);

    const thumbnailForm = document.getElementById('thumbnail-form');
    const thumbnailInput = document.getElementById('thumbnail-reference-input');
    const thumbnailTitleInput = document.getElementById('thumbnail-title');
    const thumbnailSubtitleInput = document.getElementById('thumbnail-subtitle');
    const thumbnailPriceInput = document.getElementById('thumbnail-price');
    const thumbnailErrorWrap = document.getElementById('thumbnail-error-wrap');
    const thumbnailPreviewCard = document.getElementById('thumbnail-preview-card');
    const thumbnailPreviewPick = document.getElementById('thumbnail-preview-pick');
    const thumbnailChooseImage = document.getElementById('thumbnail-choose-image');
    const thumbnailGenerateBtn = document.getElementById('btn-generate-thumbnail');
    const thumbnailDownloadLink = document.getElementById('thumbnail-download-link');

    let thumbSubmitting = false;
    let thumbReferenceImageDataUrl = '';
    let thumbImageUrl = '';

    function setThumbnailError(message) {
      if (!thumbnailErrorWrap) return;
      const text = String(message || '').trim();
      if (!text) {
        thumbnailErrorWrap.innerHTML = '';
        return;
      }
      thumbnailErrorWrap.innerHTML = `<div class="callout callout--danger"><div class="callout__copy"><strong>Error</strong><p>${esc(text)}</p></div></div>`;
    }

    function getThumbnailDisplayImageUrl() {
      return thumbImageUrl || thumbReferenceImageDataUrl || '';
    }

    function normalizeImageUrl(url) {
      return String(url || '').trim();
    }

    function uniqueUrls(urls) {
      const seen = new Set();
      const out = [];
      for (const raw of urls || []) {
        const url = normalizeImageUrl(raw);
        if (!url || seen.has(url)) continue;
        seen.add(url);
        out.push(url);
      }
      return out;
    }

    function buildThumbnailUrlCandidates() {
      const direct = [
        run.thumbnailRemoteUrl,
        run.thumbnailUrl,
      ];
      const base = 'https://fastlycb.s3.ap-southeast-2.amazonaws.com/social-media-content/reels/thumbnails';
      const stock = String(run.stockId || '').trim();
      const runIdValue = String(run.runId || '').trim();
      const exts = ['png', 'jpg', 'jpeg', 'webp'];
      const derived = [];
      for (const ext of exts) {
        if (stock) derived.push(`${base}/${encodeURIComponent(stock)}.${ext}`);
        if (runIdValue) derived.push(`${base}/${encodeURIComponent(runIdValue)}.${ext}`);
      }
      return uniqueUrls([...direct, ...derived]);
    }

    function loadImageProbe(url) {
      return new Promise((resolve) => {
        const img = new Image();
        img.onload = () => resolve(true);
        img.onerror = () => resolve(false);
        img.src = withVersionToken(url, Date.now());
      });
    }

    async function resolveExistingThumbnailUrl() {
      const candidates = buildThumbnailUrlCandidates();
      for (const candidate of candidates) {
        // eslint-disable-next-line no-await-in-loop
        const ok = await loadImageProbe(candidate);
        if (ok) return candidate;
      }
      return '';
    }

    function renderThumbnailPreview() {
      if (!thumbnailPreviewPick) return;
      const imageUrl = getThumbnailDisplayImageUrl();
      if (imageUrl) {
        const label = thumbImageUrl ? 'Generated thumbnail preview' : 'Reference image preview';
        thumbnailPreviewPick.innerHTML = `<img src="${esc(imageUrl)}" alt="${esc(label)}" onerror="this.closest('button').innerHTML='<div class=&quot;thumbnail-generator__placeholder&quot;><strong>Thumbnail unavailable</strong><span>Choose a reference image to generate a new thumbnail.</span></div>';">`;
      } else {
        thumbnailPreviewPick.innerHTML = '<div class="thumbnail-generator__placeholder"><strong>Choose reference image</strong><span>Click here to select the car photo.</span></div>';
      }
      if (thumbnailDownloadLink) {
        if (thumbImageUrl) {
          thumbnailDownloadLink.href = thumbImageUrl;
          thumbnailDownloadLink.style.display = '';
        } else {
          thumbnailDownloadLink.href = '#';
          thumbnailDownloadLink.style.display = 'none';
        }
      }
    }

    function setThumbnailSubmitting(nextState) {
      thumbSubmitting = Boolean(nextState);
      if (thumbnailGenerateBtn) {
        thumbnailGenerateBtn.disabled = thumbSubmitting;
        thumbnailGenerateBtn.textContent = thumbSubmitting ? 'Generating...' : 'Generate Thumbnail';
      }
      if (thumbnailPreviewPick) thumbnailPreviewPick.disabled = thumbSubmitting;
      if (thumbnailChooseImage) thumbnailChooseImage.disabled = thumbSubmitting;
      if (thumbnailInput) thumbnailInput.disabled = thumbSubmitting;
      if (thumbnailPreviewCard) thumbnailPreviewCard.classList.toggle('is-generating', thumbSubmitting);
      if (thumbnailPreviewPick) {
        const existingLayer = thumbnailPreviewPick.querySelector('.thumbnail-generator__processing');
        if (existingLayer) existingLayer.remove();
        if (thumbSubmitting) {
          thumbnailPreviewPick.insertAdjacentHTML(
            'beforeend',
            '<div class="thumbnail-generator__processing"><span class="thumbnail-generator__processing-darken" aria-hidden="true"></span><span class="thumbnail-generator__processing-liquid thumbnail-generator__processing-liquid--a" aria-hidden="true"></span><span class="thumbnail-generator__processing-liquid thumbnail-generator__processing-liquid--b" aria-hidden="true"></span><span class="thumbnail-generator__processing-glow" aria-hidden="true"></span><span class="thumbnail-generator__processing-scan" aria-hidden="true"></span><span class="thumbnail-generator__processing-text">Generating thumbnail...</span></div>',
          );
        }
      }
    }

    async function handleReferenceImageChange(event) {
      const file = event.target && event.target.files ? event.target.files[0] : null;
      if (!file) {
        thumbReferenceImageDataUrl = '';
        thumbImageUrl = '';
        renderThumbnailPreview();
        return;
      }
      if (!String(file.type || '').startsWith('image/')) {
        setThumbnailError('Please choose an image file.');
        thumbReferenceImageDataUrl = '';
        thumbImageUrl = '';
        renderThumbnailPreview();
        return;
      }
      try {
        const dataUrl = await fileToDataUrl(file);
        setThumbnailError('');
        thumbReferenceImageDataUrl = dataUrl;
        thumbImageUrl = '';
        renderThumbnailPreview();
      } catch (_err) {
        setThumbnailError('Could not read the selected image.');
        thumbReferenceImageDataUrl = '';
        thumbImageUrl = '';
        renderThumbnailPreview();
      }
    }

    function pickThumbnailImage() {
      if (thumbSubmitting || !thumbnailInput) return;
      thumbnailInput.click();
    }

    async function handleGenerateThumbnail(event) {
      if (event && typeof event.preventDefault === 'function') event.preventDefault();
      if (thumbSubmitting) return;

      const preparedTitle = String((thumbnailTitleInput && thumbnailTitleInput.value) || '').trim();
      const preparedSubtitle = String((thumbnailSubtitleInput && thumbnailSubtitleInput.value) || '').trim();
      const preparedPrice = String((thumbnailPriceInput && thumbnailPriceInput.value) || '').trim();

      if (!thumbReferenceImageDataUrl) {
        setThumbnailError('Reference image is required.');
        return;
      }
      if (!preparedTitle) {
        setThumbnailError('Title is required.');
        return;
      }
      if (!preparedSubtitle) {
        setThumbnailError('Subtitle is required.');
        return;
      }

      setThumbnailSubmitting(true);
      setThumbnailError('');
      try {
        const generated = await api(base + '/runs/' + encodeURIComponent(run.runId) + '/thumbnail', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            title: preparedTitle,
            subtitle: preparedSubtitle,
            price: preparedPrice,
            referenceImageDataUrl: thumbReferenceImageDataUrl,
          }),
        });
        thumbImageUrl = withVersionToken(String(generated.imageUrl || '').trim(), Date.now());
        renderThumbnailPreview();
      } catch (err) {
        setThumbnailError(err.message || String(err));
      } finally {
        setThumbnailSubmitting(false);
      }
    }

    if (thumbnailDownloadLink) {
      thumbnailDownloadLink.download = buildImageDownloadName(
        (thumbnailTitleInput && thumbnailTitleInput.value) || run.listingTitle || '',
        run.stockId || '',
        run.runId || '',
      );
    }
    void (async () => {
      const resolved = await resolveExistingThumbnailUrl();
      thumbImageUrl = resolved ? withVersionToken(resolved, Date.now()) : '';
      renderThumbnailPreview();
    })();
    setThumbnailSubmitting(false);
    if (thumbnailInput) thumbnailInput.addEventListener('change', (event) => { void handleReferenceImageChange(event); });
    if (thumbnailPreviewPick) thumbnailPreviewPick.addEventListener('click', pickThumbnailImage);
    if (thumbnailChooseImage) thumbnailChooseImage.addEventListener('click', pickThumbnailImage);
    if (thumbnailForm) thumbnailForm.addEventListener('submit', (event) => { void handleGenerateThumbnail(event); });

    const backBtn = document.getElementById('back-studio');
    if (backBtn) backBtn.onclick = () => { window.location.href = `${appBase}/workflow`; };
    const delBtn = document.getElementById('delete-run');
    if (delBtn) delBtn.onclick = async () => {
      if (!confirm('Delete this run?')) return;
      await deleteRunById(runId);
      window.location.href = `${appBase}/workflow`;
    };
    const scriptRadios = runDetailPanel.querySelectorAll('input[name="script-variant"]');
    const scriptBox = document.getElementById('script-to-stitch');
    scriptRadios.forEach((radio) => {
      radio.addEventListener('change', () => {
        if (scriptBox && radio.checked) {
          scriptBox.value = String(radio.dataset.script || '');
        }
      });
    });
    const regenBtn = document.getElementById('btn-regenerate-scripts');
    if (regenBtn) regenBtn.onclick = async () => {
      regenBtn.disabled = true;
      try {
        await api(base + '/runs/' + encodeURIComponent(runId) + '/voiceover/draft', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        });
        await loadRunDetail(runId);
      } catch (err) {
        alert(err.message || String(err));
      } finally {
        regenBtn.disabled = false;
      }
    };
    const fullBtn = document.getElementById('btn-generate-full');
    if (fullBtn) fullBtn.onclick = async () => {
      fullBtn.disabled = true;
      try {
        const script = scriptBox ? String(scriptBox.value || '').trim() : '';
        if (!script) {
          throw new Error('Pick or enter a script first.');
        }
        if (!analysisReady) {
          await api(base + '/runs/' + encodeURIComponent(runId) + '/prepare-analysis', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ script, autoComposeAfterPrepare: true }),
          });
          alert('Queued. It will now auto-run prepare + compose without another click.');
          window.location.href = `${appBase}/workflow`;
          return;
        }
        await api(base + '/runs/' + encodeURIComponent(runId) + '/compose', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ script }),
        });
        window.location.href = `${appBase}/workflow`;
      } catch (err) {
        alert(err.message || String(err));
      } finally {
        fullBtn.disabled = false;
      }
    };
  }

  function runRow(run, activeJob) {
    const s = run.stats || {};
    const latestJob = activeJob || (run && run.lastJob ? run.lastJob : null);
    const runStatus = String(run.status || '').toLowerCase();
    const jobStatus = String(latestJob && latestJob.status ? latestJob.status : '').toLowerCase();
    const progress = latestJob && latestJob.progress && typeof latestJob.progress === 'object' ? latestJob.progress : {};
    const phase = String(progress.phase || '').toLowerCase();
    const phaseLabel = String(progress.label || '').trim();
    const jobError = String(latestJob && latestJob.error ? latestJob.error : '').trim();
    const debugReason = String(run && run.debugReason ? run.debugReason : '').trim();
    const failureReason = String(run.error || jobError).trim();
    let state = 'processing';
    if (runStatus === 'failed' || runStatus === 'cancelled' || jobStatus === 'failed' || phase === 'error') {
      state = 'failed';
    } else if (jobStatus === 'queued') {
      state = 'queued';
    } else if (jobStatus === 'paused') {
      state = 'paused';
    } else if (jobStatus === 'running') {
      state = phaseLabel || {
        queued: 'queued',
        download: 'downloading clips',
        frames: 'extracting frames',
        analyze: 'analyzing clips',
        compose: 'composing reel',
        voiceover: 'stitching voice-over',
        publish: 'publishing output',
        done: 'completed',
        error: 'failed',
      }[phase] || 'running';
    } else if (jobStatus === 'completed' && !run.pipeline?.render?.done && !(run.voiceoverDraft && run.voiceoverDraft.variants && run.voiceoverDraft.variants.length)) {
      state = debugReason ? 'script generation failed' : 'script generation completed';
    } else if (run.voiceoverDraft && run.voiceoverDraft.variants && run.voiceoverDraft.variants.length) {
      state = 'scripts ready';
    } else if (runStatus === 'completed' || (run.pipeline && run.pipeline.render && run.pipeline.render.done)) {
      state = 'video ready';
    } else if (run.pipeline && run.pipeline.analyze && run.pipeline.analyze.done) {
      state = 'prepared for compose';
    }
    return `<article class="run-row">
      <div class="run-row__main">
        <div class="run-row__header">
          <strong class="run-row__title">${esc(run.listingTitle || run.runId)}</strong>
          <span class="run-row__meta">${esc(new Date(run.createdAt || run.updatedAt || Date.now()).toLocaleString())} | ${esc(run.stockId || '-')} | ${esc(run.runId || '-')}</span>
        </div>
        <div class="pipeline-dots"><span class="pipeline-dot ${(run.pipeline&&run.pipeline.download&&run.pipeline.download.done)?'pipeline-dot--on':''}"></span><span class="pipeline-dot ${(run.pipeline&&((run.pipeline.frames&&run.pipeline.frames.done)||(run.pipeline.prepare&&run.pipeline.prepare.done)))?'pipeline-dot--on':''}"></span><span class="pipeline-dot ${(run.pipeline&&run.pipeline.analyze&&run.pipeline.analyze.done)?'pipeline-dot--on':''}"></span><span class="pipeline-dot ${(run.pipeline&&run.pipeline.render&&run.pipeline.render.done)?'pipeline-dot--on':''}"></span></div>
        <div class="run-row__stats"><span>${state}</span><span>${s.downloads||0} clips</span><span>${s.frames||0} frames</span><span>${s.analyzed||0} AI</span><span>${s.planned||0} cut</span></div>
        ${((runStatus === 'failed' || runStatus === 'cancelled' || jobStatus === 'failed' || phase === 'error') && failureReason) ? `<div class="run-row__error"><strong>Failure reason:</strong> ${esc(failureReason)}</div>` : ''}
        ${(debugReason && !failureReason) ? `<div class="run-row__error"><strong>Reason:</strong> ${esc(debugReason)}</div>` : ''}
      </div>
      <div class="run-row__actions">
        <button class="button button--secondary" type="button" data-view="${esc(run.runId)}">View</button>
        <button class="button button--danger" type="button" data-del="${esc(run.runId)}">Del</button>
      </div>
    </article>`;
  }

  async function submitRun(e) {
    e.preventDefault();
    const fd = new FormData(form);
    const payload = Object.fromEntries(fd.entries());
    payload.command = 'script-draft';
    payload.compose = false;
    payload.voiceoverScriptApproval = true;
    payload.maxClips = null;
    payload.headful = false;
    const url = String(payload.url || '').trim();
    if (!url) {
      formStatus.textContent = 'Please provide at least one URL.';
      return;
    }
    payload.url = url;
    try {
      formStatus.textContent = 'Queueing run...';
      await api(base + '/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      formStatus.textContent = 'Queued.';
      await loadCurrentJob();
      await loadRuns();
    } catch (err) {
      formStatus.textContent = err.message;
    }
  }

  async function refreshInventory() {
    invNote.textContent = 'Refreshing inventory cache...';
    await api(base + '/vehicle-inventory/refresh', { method: 'POST' });
    const s = await api(base + '/vehicle-inventory/status');
    invNote.textContent = `Loaded ${s.count || 0} items.`;
  }

  async function searchInventory() {
    const q = invQ.value.trim();
    if (!q) { invResults.style.display='none'; invResults.innerHTML=''; return; }
    const data = await api(base + '/vehicle-inventory/search?q=' + encodeURIComponent(q) + '&limit=20');
    const list = data.matches || [];
    latestInventoryMatches = list;
    if (!list.length) {
      invResults.style.display='block';
      invResults.innerHTML='<div class="vehicle-lookup__status">Cache empty. Click refresh.</div>';
      return;
    }
    invResults.style.display='block';
    invResults.innerHTML = '<div class="vehicle-lookup__results">' + list.map((v, i) => {
      const img = firstImage(v);
      const subtitle = [v.year, v.make, v.model].filter(Boolean).join(' ') || (v.bodyType || '');
      return `<button type="button" class="vehicle-option" data-idx="${i}">
        <span class="vehicle-option__thumb">${img ? `<img src="${esc(img)}" alt="">` : '<span class="vehicle-option__placeholder"></span>'}</span>
        <span class="vehicle-option__main"><strong class="vehicle-option__title">${esc(v.title||'-')}</strong><span class="vehicle-option__meta">${esc(v.stockNo||'-')}${subtitle ? ' • ' + esc(subtitle) : ''}</span></span>
        <span class="vehicle-option__price">${esc(formatPrice(v) || '')}</span>
      </button>`;
    }).join('') + '</div>';
    invResults.querySelectorAll('.vehicle-option').forEach(btn => btn.onclick = () => {
      const idx = Number(btn.dataset.idx);
      const v = latestInventoryMatches[idx];
      if (!v) return;
      form.elements.stockId.value = v.stockNo || '';
      form.elements.listingTitle.value = (v.title || '').trim();
      form.elements.listingPrice.value = formatPrice(v);
      form.elements.carDescription.value = buildDescription(v);
      if (form.elements.priceIncludes) form.elements.priceIncludes.value = buildPriceIncludes(v);
      invResults.style.display='none';
      const stock = String(v.stockNo || '').trim();
      const title = String((v.title || '').trim());
      invQ.value = stock && title ? `Stock ${stock} - ${title}` : `${stock} ${title}`.trim();
    });
  }

  function formatPrice(v) {
    const raw = v.salePrice ?? v.price ?? '';
    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) return `AU$${n.toLocaleString('en-AU')}`;
    return '';
  }

  function buildDescription(v) {
    let text = String(v.description || '').trim();
    if (!text) {
      const fallback = [];
      if (v.stockNo || v.year || v.make || v.model) {
        fallback.push(`Stock - ${(v.stockNo || '')}${(v.year || v.make || v.model) ? ` ${[v.year, v.make, v.model].filter(Boolean).join(' ')}` : ''}`.trim());
      }
      if (v.odometer) fallback.push(`${v.odometer} KM`);
      text = fallback.join('\n');
    }
    return text.replace(/\\n/g, '\n').replace(/\r\n/g, '\n');
  }

  function buildPriceIncludes(v) {
    const src = String(v.description || '').replace(/\\n/g, '\n');
    const lines = src.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
    const keep = [];
    const rx = /(registration|warranty|roadworthy|service|insurance|delivery|serviced)/i;
    for (const line of lines) {
      if (rx.test(line)) keep.push(line.replace(/^[-•\s]+/, '').trim());
    }
    if (!keep.length) {
      const out = [];
      if (/6 months nsw registration/i.test(src)) out.push('6 Months NSW Registration');
      if (/roadworthy/i.test(src)) out.push('Fresh Roadworthy Certificate');
      if (/insurance/i.test(src)) out.push('CTP Insurance');
      return out.join('\n');
    }
    return [...new Set(keep)].join('\n');
  }

  function firstImage(v) {
    const lists = [v.exteriorPhoto, v.interiorPhoto, v.auctionPhotos, v.images];
    for (const list of lists) {
      if (Array.isArray(list) && list.length && list[0]) return String(list[0]);
    }
    return '';
  }

  let t;
  if (invQ) {
    invQ.addEventListener('input', () => {
      clearTimeout(t);
      t = setTimeout(() => searchInventory().catch((e) => { if (invNote) invNote.textContent = e.message; }), 250);
    });
  }
  if (invClear) {
    invClear.addEventListener('click', () => {
      invQ.value = '';
      invResults.style.display = 'none';
      invResults.innerHTML = '';
      form.elements.stockId.value = '';
      form.elements.listingTitle.value = '';
      form.elements.listingPrice.value = '';
      form.elements.carDescription.value = '';
      if (form.elements.priceIncludes) form.elements.priceIncludes.value = '';
      invNote.textContent = '';
    });
  }

  if (invRefresh) {
    invRefresh.addEventListener('click', () => refreshInventory().catch((e) => { if (invNote) invNote.textContent = e.message; }));
  }
  if (runsRefresh) {
    runsRefresh.addEventListener('click', () => loadRuns().catch((e) => { if (formStatus) formStatus.textContent = e.message; }));
  }
  if (runsTrashDelete) {
    const runTrashHandler = () => {
      if (formStatus) formStatus.textContent = 'Preparing trash cleanup...';
      return deleteTrash().catch((e) => {
        if (formStatus) formStatus.textContent = e.message;
      });
    };
    runsTrashDelete.addEventListener('click', runTrashHandler);
    runsTrashDelete.onclick = runTrashHandler;
  }
  if (form) {
    form.addEventListener('submit', submitRun);
  }

  if (currentRunId) {
    loadRunDetail(currentRunId).catch(e => {
      if (runDetailPanel) {
        runDetailPanel.style.display = 'block';
        runDetailPanel.innerHTML = `<div class="empty-block"><strong>${esc(e.message)}</strong></div>`;
      }
    });
  } else {
    setStudioModeUi();
    loadCurrentJob().catch(() => {});
    loadRuns().catch(e => formStatus.textContent = e.message);
  }
  window.setInterval(() => {
    if (!currentRunId) {
      loadCurrentJob().catch(() => {});
      loadRuns().catch(() => {});
    }
  }, 2500);

  if (navStudio) navStudio.addEventListener('click', () => { window.location.href = `${appBase}/workflow`; });
  if (navRuns) navRuns.addEventListener('click', () => { window.location.href = `${appBase}/runs`; });
})();
