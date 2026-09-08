"""
06_RepairMissingRoutes.py
=========================
Find Run365 activities whose exported GPX / TCX / KML files carry no GPS
coordinates (so nothing can be rendered on a map), then rebuild a route for
each of them.

Why some activities have no route
---------------------------------
The raw FIT files inside the Garmin account export
(DI_CONNECT/DI-Connect-Fitness-Uploaded-Files/UploadedFiles_0-_Part1.zip)
contain ZERO position records for those activities, i.e. the watch never
recorded GPS for them (the barometric altitude of ~-305 m confirms the GPS
never locked / was switched off).  The data therefore cannot be "recovered";
it can only be *reconstructed*.

How the reconstruction works
----------------------------
Every affected activity still has a complete accelerometer track in its TCX:
one Trackpoint every few seconds with Time, cumulative DistanceMeters, Speed
and RunCadence.  We take a *reference* activity that was recorded with GPS on
the same daily loop, parameterise its GPX track by distance, and place each
affected Trackpoint on that reference polyline at the matching distance.

Two projection modes are available (CFG_PROJECTION_MODE):
  * 'scale'    - the affected run is assumed to be the SAME full loop as the
                 reference; cumulative distance is mapped proportionally
                 (wrist-only distance under-reads by ~10-15 % vs GPS, see the
                 stride analysis in Garmin/missing_routes_report.md).
  * 'absolute' - metres are mapped 1:1 and the route simply stops early when
                 the affected run is shorter than the reference.

Everything produced here is written to a SEPARATE folder
(Garmin/repaired/{gpx,tcx,kml}/) and every file is annotated as reconstructed.
Original files under Garmin/*/bak/ are never modified.

Usage
-----
    python3 06_RepairMissingRoutes.py            # scan + report + repair
    python3 06_RepairMissingRoutes.py --scan     # scan + report only
    python3 06_RepairMissingRoutes.py --no-fit   # skip FIT temperature lookup

Optional dependency: `fitdecode` (pip install fitdecode) - only used to copy
the real air-temperature readings from the raw FIT file into the rebuilt GPX.
"""
import os
import re
import sys
import glob
import json
import math
import bisect
import zipfile
from datetime import datetime, timedelta, timezone

# ------------------------------------------------------------------ Config --
CFG_GARMIN_DIR = './Garmin/'
CFG_SRC_SUBDIR = 'bak'                                   # where the originals live
CFG_OUT_DIR = CFG_GARMIN_DIR + 'repaired/'
CFG_REPORT_MD = CFG_GARMIN_DIR + 'missing_routes_report.md'
CFG_REPORT_JSON = CFG_GARMIN_DIR + 'missing_routes_report.json'
CFG_FIT_ZIP = CFG_GARMIN_DIR + 'fa4684ae-7056-4a8f-a0e3-ced66ee5e9c8_1/DI_CONNECT/DI-Connect-Fitness-Uploaded-Files/UploadedFiles_0-_Part1.zip'

CFG_YEAR = 2021                                          # Run365 challenge year (Day 1 = 1 Jan)
CFG_LOCAL_TZ = timezone(timedelta(hours=8))              # Asia/Hong_Kong (no DST)

# Reference activity used when no per-activity override is given.
# 2021-01-27 (activity 6188052022): standard Kowloon loop, GPS locked almost
# immediately, so its track covers 97 % of the recorded distance.
CFG_DEFAULT_REFERENCE_ID = '6188052022'
CFG_REFERENCE_OVERRIDE = {
    # '6055376813': '6077490486',   # example: use 2021-01-09 for Day 1
}
CFG_PROJECTION_MODE = 'scale'                            # 'scale' | 'absolute'

CFG_FIT_TIME_MATCH_SEC = 120                             # FIT file_id vs activity start tolerance
CFG_RECONSTRUCTED_TAG = 'ROUTE RECONSTRUCTED'

FMT_GPX, FMT_TCX, FMT_KML = 'gpx', 'tcx', 'kml'
EARTH_RADIUS_M = 6371000.0

TCX_NS = 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2'
GPX_NS = 'http://www.topografix.com/GPX/1/1'
KML_NS = 'http://earth.google.com/kml/2.1'


# ----------------------------------------------------------------- Helpers --
def _haversine_m(a, b):
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    d_lat, d_lon = lat2 - lat1, lon2 - lon1
    h = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(h))


def _src_path(fmt, activity_id):
    return os.path.join(CFG_GARMIN_DIR, fmt, CFG_SRC_SUBDIR, 'activity_{}.{}'.format(activity_id, fmt))


def _out_path(fmt, activity_id):
    return os.path.join(CFG_OUT_DIR, fmt, 'activity_{}.{}'.format(activity_id, fmt))


def _read(path):
    with open(path, encoding='utf-8') as fh:
        return fh.read()


def _write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(text)


def _parse_utc(iso):
    # 2020-12-31T23:10:55.000Z
    return datetime.strptime(iso, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)


def _fmt_utc(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _fmt_local(dt):
    return dt.astimezone(CFG_LOCAL_TZ).strftime('%Y-%m-%dT%H:%M:%S+08:00')


def _day_of_challenge(dt_utc):
    local = dt_utc.astimezone(CFG_LOCAL_TZ)
    return (local.date() - datetime(CFG_YEAR, 1, 1).date()).days + 1


def _hms(seconds):
    return str(timedelta(seconds=int(round(seconds))))


# ------------------------------------------------------------------- Scan --
def scan_missing_routes():
    """Return a list of activities that have no coordinates in GPX/TCX/KML."""
    ids = sorted(os.path.basename(p)[len('activity_'):-len('.tcx')]
                 for p in glob.glob(_src_path(FMT_TCX, '*')))
    missing = []
    for activity_id in ids:
        gpx_text = _read(_src_path(FMT_GPX, activity_id))
        tcx_text = _read(_src_path(FMT_TCX, activity_id))
        kml_text = _read(_src_path(FMT_KML, activity_id))

        gpx_pts = len(re.findall(r'<trkpt\b', gpx_text))
        tcx_pts = len(re.findall(r'<Trackpoint>', tcx_text))
        tcx_pos = len(re.findall(r'<Position>', tcx_text))
        kml_line = '<LineString>' in kml_text
        if gpx_pts > 0 and tcx_pos > 0 and kml_line:
            continue

        start = _parse_utc(re.search(r'<Id>([^<]+)</Id>', tcx_text).group(1))
        laps = re.findall(r'<Lap StartTime="[^"]+">\s*<TotalTimeSeconds>([^<]+)</TotalTimeSeconds>\s*<DistanceMeters>([^<]+)</DistanceMeters>', tcx_text)
        total_sec = sum(float(t) for t, _ in laps)
        total_m = sum(float(d) for _, d in laps)
        missing.append({
            'Day': _day_of_challenge(start),
            'Date': start.astimezone(CFG_LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            'Activity ID': activity_id,
            'Total Time': _hms(total_sec),
            'Distance (km)': round(total_m / 1000, 2),
            'Laps': len(laps),
            'GPX trkpt': gpx_pts,
            'TCX Trackpoint': tcx_pts,
            'TCX Position': tcx_pos,
            'KML LineString': kml_line,
        })
    missing.sort(key=lambda r: r['Day'])
    return missing


def write_report(missing, repaired_info=None):
    lines = ['# Run365 - activities without a GPS route', '',
             'Generated by `06_RepairMissingRoutes.py`. Source files: `Garmin/{gpx,tcx,kml}/bak/`.', '',
             '## 1. Activities found without coordinates', '',
             '| Day | Date | Activity ID | Total Time | Distance (km) | Laps | GPX trkpt | TCX Trackpoint | TCX Position | KML LineString |',
             '|-----|------|-------------|------------|---------------|------|-----------|----------------|--------------|----------------|']
    for r in missing:
        lines.append('| {Day} | {Date} | {Activity ID} | {Total Time} | {Distance (km)} | {Laps} | {GPX trkpt} | {TCX Trackpoint} | {TCX Position} | {KML LineString} |'.format(**r))
    lines += ['',
              '## 2. Root cause', '',
              '* The raw FIT files in the Garmin account export contain **0 position records** for these activities.',
              '* The activities were recorded as outdoor `running` / `generic` on the fenix 3, but the GPS never locked '
              '(barometric altitude sits at about -305 m, i.e. never calibrated by a fix).',
              '* Distance / speed / cadence for these days come from the wrist accelerometer only.',
              '* Conclusion: the GPS track was never recorded, so it cannot be recovered from any source - it can only be reconstructed.', '']
    if repaired_info:
        lines += ['## 3. Reconstruction output', '',
                  'Projection mode: `{}`. Files written to `{}`; originals untouched.'.format(CFG_PROJECTION_MODE, CFG_OUT_DIR), '',
                  '| Day | Activity ID | Reference activity | Reference date | Ref. track (km) | Trackpoints placed | Temp. from FIT |',
                  '|-----|-------------|--------------------|----------------|-----------------|--------------------|----------------|']
        for r in repaired_info:
            lines.append('| {Day} | {Activity ID} | {Reference ID} | {Reference Date} | {Reference km} | {Trackpoints} | {Temp from FIT} |'.format(**r))
        lines += ['',
                  '**Assumption:** each affected run followed the same loop as its reference activity. '
                  'The reconstructed route is a best-effort estimate for map rendering, not a GPS record. '
                  'Change `CFG_REFERENCE_OVERRIDE` / `CFG_PROJECTION_MODE` and re-run to use a different reference.', '']
    _write(CFG_REPORT_MD, '\n'.join(lines))
    _write(CFG_REPORT_JSON, json.dumps({'missing': missing, 'repaired': repaired_info or []}, indent=1))


# ----------------------------------------------------------- Reference GPX --
class ReferenceRoute:
    """GPX track parameterised by cumulative distance for interpolation."""

    def __init__(self, activity_id):
        self.activity_id = activity_id
        text = _read(_src_path(FMT_GPX, activity_id))
        self.start_utc = _parse_utc(re.search(r'<time>([^<]+)</time>', text).group(1))
        pts = re.findall(r'<trkpt lat="([^"]+)" lon="([^"]+)">\s*<ele>([^<]+)</ele>', text)
        if len(pts) < 2:
            raise ValueError('Reference activity {} has no usable GPS track'.format(activity_id))
        self.points = [(float(la), float(lo), float(el)) for la, lo, el in pts]
        self.cum = [0.0]
        for prev, cur in zip(self.points, self.points[1:]):
            self.cum.append(self.cum[-1] + _haversine_m(prev[:2], cur[:2]))
        self.length_m = self.cum[-1]

    def position_at(self, metres):
        """Linear interpolation of (lat, lon, ele) at a distance along the track."""
        metres = max(0.0, min(metres, self.length_m))
        j = bisect.bisect_right(self.cum, metres) - 1
        if j >= len(self.points) - 1:
            return self.points[-1]
        seg = self.cum[j + 1] - self.cum[j]
        frac = (metres - self.cum[j]) / seg if seg > 0 else 0.0
        p, q = self.points[j], self.points[j + 1]
        return tuple(p[k] + frac * (q[k] - p[k]) for k in range(3))


# ------------------------------------------------------- FIT temperature --
def load_fit_temperatures(start_utc, enabled=True):
    """Return {utc datetime: temperature} from the raw FIT matching this activity."""
    if not enabled or not os.path.exists(CFG_FIT_ZIP):
        return {}
    try:
        import fitdecode
    except ImportError:
        print('  ! fitdecode not installed - air temperature will be omitted from GPX')
        return {}
    with zipfile.ZipFile(CFG_FIT_ZIP) as zf:
        for name in zf.namelist():
            if not name.endswith('.fit'):
                continue
            with zf.open(name) as fh:
                temps = {}
                try:
                    for frame in fitdecode.FitReader(fh):
                        if not isinstance(frame, fitdecode.FitDataMessage):
                            continue
                        if frame.name == 'file_id':
                            created = frame.get_value('time_created', fallback=None)
                            if frame.get_value('type', fallback=None) != 'activity' or created is None \
                                    or abs((created - start_utc).total_seconds()) > CFG_FIT_TIME_MATCH_SEC:
                                break
                        elif frame.name == 'record':
                            ts = frame.get_value('timestamp', fallback=None)
                            temp = frame.get_value('temperature', fallback=None)
                            if ts is not None and temp is not None:
                                temps[ts] = temp
                except Exception as exc:                        # corrupt / unreadable FIT
                    print('  ! could not read {}: {}'.format(name, exc))
                    continue
                if temps:
                    print('  temperature source: {} ({} records)'.format(name, len(temps)))
                    return temps
    return {}


# ------------------------------------------------------------ TCX source --
def parse_tcx_trackpoints(tcx_text):
    """Return (laps, trackpoints) from the accelerometer-only TCX."""
    laps = []
    for lap_block in re.findall(r'<Lap StartTime="[^"]+">.*?</Lap>', tcx_text, flags=re.S):
        laps.append({
            'seconds': float(re.search(r'<TotalTimeSeconds>([^<]+)<', lap_block).group(1)),
            'metres': float(re.search(r'<DistanceMeters>([^<]+)<', lap_block).group(1)),
        })
    trackpoints = []
    for block in re.findall(r'<Trackpoint>.*?</Trackpoint>', tcx_text, flags=re.S):
        cad = re.search(r'<ns3:RunCadence>([^<]+)<', block)
        trackpoints.append({
            'time': _parse_utc(re.search(r'<Time>([^<]+)<', block).group(1)),
            'metres': float(re.search(r'<DistanceMeters>([^<]+)<', block).group(1)),
            'cad': int(cad.group(1)) if cad else None,
        })
    return laps, trackpoints


def _projection(total_m, ref):
    if CFG_PROJECTION_MODE == 'scale':
        factor = ref.length_m / total_m if total_m > 0 else 1.0
        return lambda metres: metres * factor
    if CFG_PROJECTION_MODE == 'absolute':
        return lambda metres: metres
    raise ValueError('Unknown CFG_PROJECTION_MODE: {}'.format(CFG_PROJECTION_MODE))


# ------------------------------------------------------------- Builders --
def build_gpx(activity_id, start_utc, trackpoints, note, temps):
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<gpx creator="Garmin Connect" version="1.1"',
             '  xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/11.xsd"',
             '  xmlns:ns3="http://www.garmin.com/xmlschemas/TrackPointExtension/v1"',
             '  xmlns="{}"'.format(GPX_NS),
             '  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns2="http://www.garmin.com/xmlschemas/GpxExtensions/v3">',
             '  <metadata>',
             '    <link href="connect.garmin.com">',
             '      <text>Garmin Connect</text>',
             '    </link>',
             '    <time>{}</time>'.format(_fmt_utc(start_utc)),
             '  </metadata>',
             '  <trk>',
             '    <name>Running</name>',
             '    <desc>{}</desc>'.format(note),
             '    <type>running</type>',
             '    <trkseg>']
    for tp in trackpoints:
        lat, lon, ele = tp['pos']
        lines += ['      <trkpt lat="{!r}" lon="{!r}">'.format(lat, lon),
                  '        <ele>{!r}</ele>'.format(round(ele, 2)),
                  '        <time>{}</time>'.format(_fmt_utc(tp['time'])),
                  '        <extensions>',
                  '          <ns3:TrackPointExtension>']
        temp = temps.get(tp['time'])
        if temp is not None:
            lines.append('            <ns3:atemp>{:.1f}</ns3:atemp>'.format(temp))
        if tp['cad'] is not None:
            lines.append('            <ns3:cad>{}</ns3:cad>'.format(tp['cad']))
        lines += ['          </ns3:TrackPointExtension>',
                  '        </extensions>',
                  '      </trkpt>']
    lines += ['    </trkseg>', '  </trk>', '</gpx>', '']
    return '\n'.join(lines)


def build_tcx(tcx_text, trackpoints, note):
    positions = iter(trackpoints)

    def _patch(match):
        lat, lon, ele = next(positions)['pos']
        indent = match.group('indent')
        return ('{i}<Time>{t}</Time>\n'
                '{i}<Position>\n'
                '{i}  <LatitudeDegrees>{lat!r}</LatitudeDegrees>\n'
                '{i}  <LongitudeDegrees>{lon!r}</LongitudeDegrees>\n'
                '{i}</Position>\n'
                '{i}<AltitudeMeters>{ele!r}</AltitudeMeters>').format(i=indent, t=match.group('time'), lat=lat, lon=lon, ele=round(ele, 2))

    pattern = r'(?P<indent>[ \t]*)<Time>(?P<time>[^<]+)</Time>\n[ \t]*<AltitudeMeters>[^<]+</AltitudeMeters>'
    patched, count = re.subn(pattern, _patch, tcx_text)
    if count != len(trackpoints):
        raise RuntimeError('TCX patch mismatch: {} trackpoints, {} patched'.format(len(trackpoints), count))
    notes = '      <Notes>{}</Notes>\n      <Creator'.format(note)
    patched = patched.replace('      <Creator', notes, 1)
    return patched


def _kml_point_placemark(name, description, href, scale, hotspot_x, lat, lon):
    return ('      <Placemark>\n'
            '        <name>{name}</name>\n'
            '        <description>{desc}</description>\n'
            '        <Style>\n'
            '          <IconStyle>\n'
            '            <scale>{scale}</scale>\n'
            '            <Icon>\n'
            '              <href>{href}</href>\n'
            '            </Icon>\n'
            '            <hotSpot x="{hx}" xunits="fraction" y="0.0" yunits="fraction"/>\n'
            '          </IconStyle>\n'
            '        </Style>\n'
            '        <Point>\n'
            '          <coordinates>{lon!r}, {lat!r}</coordinates>\n'
            '        </Point>\n'
            '      </Placemark>\n').format(name=name, desc=description, scale=scale, href=href, hx=hotspot_x, lat=lat, lon=lon)


def build_kml(kml_text, trackpoints, lap_positions, note):
    first, last = trackpoints[0]['pos'], trackpoints[-1]['pos']
    coords = ' '.join('{!r},{!r}'.format(tp['pos'][1], tp['pos'][0]) for tp in trackpoints)
    track = ('    <description>{note}</description>\n'
             '    <Placemark>\n'
             '      <name>Track</name>\n'
             '      <Style>\n'
             '        <LineStyle>\n'
             '          <color>FF0000FF</color>\n'
             '          <width>3.0</width>\n'
             '        </LineStyle>\n'
             '      </Style>\n'
             '      <LineString>\n'
             '        <extrude>false</extrude>\n'
             '        <tessellate>true</tessellate>\n'
             '        <altitudeMode>clampToGround</altitudeMode>\n'
             '        <coordinates>{coords}</coordinates>\n'
             '      </LineString>\n'
             '    </Placemark>\n').format(note=note, coords=coords)
    out = kml_text.replace('    <name>Running</name>\n', '    <name>Running</name>\n' + track, 1)

    # Laps folder: Start marker, real lap coordinates, End marker
    start_pm = _kml_point_placemark('Start', note, r'explore\image\main\icons\kml\grn-play.png', '1.3', '0.5', first[0], first[1])
    end_pm = _kml_point_placemark('End', note, 'http://maps.google.com/mapfiles/kml/paddle/red-square.png', '1.0', '0.5', last[0], last[1])
    lap_anchor = '      <Placemark>\n        <name>Lap 1</name>'
    out = out.replace(lap_anchor, start_pm + lap_anchor, 1)
    lap_iter = iter(lap_positions)
    out, count = re.subn(r'<coordinates>null,null</coordinates>',
                         lambda m: '<coordinates>{!r},{!r}</coordinates>'.format(*next(lap_iter)[1::-1]), out)
    if count != len(lap_positions):
        raise RuntimeError('KML lap patch mismatch: {} laps, {} patched'.format(len(lap_positions), count))
    track_points_anchor = '    </Folder>\n    <Folder>\n      <name>Track Points</name>'
    out = out.replace(track_points_anchor, end_pm + track_points_anchor, 1)

    # Track Points folder: one animated placemark per segment
    segments = []
    for cur, nxt in zip(trackpoints, trackpoints[1:]):
        lat, lon, ele = cur['pos']
        segments.append('      <Placemark>\n'
                        '        <TimeSpan>\n'
                        '          <begin>{b}</begin>\n'
                        '          <end>{e}</end>\n'
                        '        </TimeSpan>\n'
                        '        <Style>\n'
                        '          <IconStyle>\n'
                        '            <color>FF00FFFF</color>\n'
                        '            <scale>0.9</scale>\n'
                        '            <Icon>\n'
                        '              <href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href>\n'
                        '            </Icon>\n'
                        '          </IconStyle>\n'
                        '        </Style>\n'
                        '        <Point>\n'
                        '          <altitudeMode>clampToGround</altitudeMode>\n'
                        '          <coordinates>{lon!r},{lat!r}, {ele!r}</coordinates>\n'
                        '        </Point>\n'
                        '      </Placemark>\n'.format(b=_fmt_local(cur['time']), e=_fmt_local(nxt['time']), lat=lat, lon=lon, ele=round(ele, 2)))
    tp_folder_end = '      </Style>\n    </Folder>\n  </Folder>\n</kml>'
    if tp_folder_end not in out:
        raise RuntimeError('KML Track Points folder anchor not found')
    out = out.replace(tp_folder_end, '      </Style>\n' + ''.join(segments) + '    </Folder>\n  </Folder>\n</kml>', 1)
    return out


# -------------------------------------------------------------- Repair --
def repair_activity(entry, use_fit=True):
    activity_id = entry['Activity ID']
    ref_id = CFG_REFERENCE_OVERRIDE.get(activity_id, CFG_DEFAULT_REFERENCE_ID)
    ref = ReferenceRoute(ref_id)
    print('Day {:>3}  activity {}  <-  reference {} ({} km track)'.format(entry['Day'], activity_id, ref_id, round(ref.length_m / 1000, 2)))

    tcx_text = _read(_src_path(FMT_TCX, activity_id))
    kml_text = _read(_src_path(FMT_KML, activity_id))
    start_utc = _parse_utc(re.search(r'<Id>([^<]+)</Id>', tcx_text).group(1))
    laps, trackpoints = parse_tcx_trackpoints(tcx_text)

    total_m = max(sum(lap['metres'] for lap in laps), trackpoints[-1]['metres'])
    project = _projection(total_m, ref)
    for tp in trackpoints:
        tp['pos'] = ref.position_at(project(tp['metres']))
    lap_positions, cum = [], 0.0
    for lap in laps:
        cum += lap['metres']
        lap_positions.append(ref.position_at(project(cum)))

    note = ('{tag}: no GPS was recorded for this activity. Coordinates were projected from the '
            'accelerometer distance onto the GPS track of activity {ref} ({ref_date}) using mode "{mode}". '
            'Generated by 06_RepairMissingRoutes.py.').format(
        tag=CFG_RECONSTRUCTED_TAG, ref=ref_id, ref_date=ref.start_utc.astimezone(CFG_LOCAL_TZ).strftime('%Y-%m-%d'), mode=CFG_PROJECTION_MODE)

    temps = load_fit_temperatures(start_utc, enabled=use_fit)

    _write(_out_path(FMT_GPX, activity_id), build_gpx(activity_id, start_utc, trackpoints, note, temps))
    _write(_out_path(FMT_TCX, activity_id), build_tcx(tcx_text, trackpoints, note))
    _write(_out_path(FMT_KML, activity_id), build_kml(kml_text, trackpoints, lap_positions, note))

    return {
        'Day': entry['Day'],
        'Activity ID': activity_id,
        'Reference ID': ref_id,
        'Reference Date': ref.start_utc.astimezone(CFG_LOCAL_TZ).strftime('%Y-%m-%d'),
        'Reference km': round(ref.length_m / 1000, 2),
        'Trackpoints': len(trackpoints),
        'Temp from FIT': 'yes' if temps else 'no',
    }


def main(argv):
    scan_only = '--scan' in argv
    use_fit = '--no-fit' not in argv

    missing = scan_missing_routes()
    print('Activities without GPS coordinates: {}'.format(len(missing)))
    for r in missing:
        print('  Day {Day:>3}  {Date}  {Activity ID}  {Total Time}  {Distance (km)} km  laps={Laps}'.format(**r))
    if scan_only or not missing:
        write_report(missing)
        print('Report written to {}'.format(CFG_REPORT_MD))
        return

    repaired = [repair_activity(entry, use_fit=use_fit) for entry in missing]
    write_report(missing, repaired)
    print('Repaired files written to {}'.format(CFG_OUT_DIR))
    print('Report written to {}'.format(CFG_REPORT_MD))


if __name__ == '__main__':
    main(sys.argv[1:])
