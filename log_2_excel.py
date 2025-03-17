import re
import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

############################################################
## Utility Functions
############################################################

def format_time_duration(seconds):
    """Format seconds as HH:MM:SS."""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def extract_timestamp_line(line):
    """
    Extract timestamp and full line from a log line.
    Returns (datetime object, line text) or (None, line) if no timestamp found.
    """
    try:
        timestamp_str = line.split(" - ")[0].strip()
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        return timestamp, line.strip()
    except (IndexError, ValueError):
        return None, line

############################################################
## calculate_break_times
############################################################

def calculate_break_times(all_sessions):
    """
    Calculate break times between sessions for each user.
    A 'break' is the time between the end of one session and the start of the next
    for that same user, on the same date only.

    Returns a list of dicts containing:
      User, Date, Start Time, End Time, Break Time (HH:MM:SS), Break in seconds
    """
    break_times = []
    user_sessions = defaultdict(list)

    # Group sessions by user
    for session in all_sessions:
        if session["start_time"] and session["end_time"]:
            user_sessions[session["user"]].append(session)
    
    # For each user, sort sessions by start_time and find gaps
    for user, sessions in user_sessions.items():
        sessions.sort(key=lambda x: x["start_time"])

        for i in range(len(sessions) - 1):
            current_session = sessions[i]
            next_session = sessions[i + 1]

            # Only consider break if the date is the same
            if current_session["date"] == next_session["date"]:
                break_start = current_session["end_time"]
                break_end = next_session["start_time"]
                gap_secs = (break_end - break_start).total_seconds()

                if gap_secs > 0:
                    break_times.append({
                        "User": user,
                        "Date": current_session["date"],
                        "Starting": break_start.strftime("%H:%M:%S"),
                        "Ending": break_end.strftime("%H:%M:%S"),
                        "Break Time": format_time_duration(int(gap_secs)),
                        "Break in seconds": int(gap_secs),
                        "Log File": current_session["log_file"]
                    })
    return break_times

############################################################
## analyze_log_file
############################################################

def analyze_log_file(log_file_path):
    """
    Analyzes a single log file, returning:
      sessions, ocr_data, shortcut_data, image_record_data, field_updates_data
    """
    login_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (?:config) - INFO - Logging initialized for user: (.+) on (\d{4}-\d{2}-\d{2})"
    )
    image_update_pattern = re.compile(r"Updated IMAGE_NUMBER to (\d+)_\d+ for all records of (\d+)")
    edit_pattern = re.compile(r"UPDATED (\w+) .+ TO (.+?) of (\d+)")
    r_num_pattern = re.compile(r"UPDATED r_num\s+TO (\d+) of (\d+)")
    doc_type_update_pattern = re.compile(r"Updated DOC_TYPE for (\d+) local records")
    any_update_pattern = re.compile(r"UPDATED")
    text_clipboard_pattern = re.compile(r"Text copied to clipboard: '(.+)'")
    ocr_image_pattern = re.compile(r"Updated IMAGE_NUMBER to (\d+)_00(\d+) for all records of (\d+)")
    shortcut_pattern = re.compile(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - scripts\.config - INFO - ([\w+]+) pressed"
    )
    image_pattern_sheet5 = re.compile(r"Updated IMAGE_NUMBER to \d+_\d+ for all records of (\d+)")
    record_pattern_sheet5 = re.compile(r"of (\d+)$")
    field_update_pattern = re.compile(r"UPDATED (\w+)")

    # OCR patterns
    ocr_start_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - scripts\.config - INFO - HWR mode set to True"
    )
    ocr_end_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - scripts\.config - DEBUG - Text copied to clipboard: '(.+)'"
    )

    sessions = []
    current_session = None
    ocr_records = {}
    shortcuts = {}
    image_record_map = defaultdict(set)

    field_updates = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    current_user = None
    current_date = None
    current_image = None

    current_ocr_start_time = None
    current_ocr_image_id = None
    ocr_in_progress = False
    ocr_durations = defaultdict(list)
    ocr_durations_with_criteria = defaultdict(list)

    try:
        with open(log_file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {log_file_path}: {e}")
        return [], [], {}, [], []

    for i, line in enumerate(lines):
        # Check for OCR start
        start_match = ocr_start_pattern.search(line)
        if start_match:
            current_ocr_start_time = datetime.strptime(start_match.group(1), "%Y-%m-%d %H:%M:%S")
            ocr_in_progress = True

        # Check for OCR end
        end_match = ocr_end_pattern.search(line)
        if end_match and current_ocr_start_time and current_ocr_image_id and ocr_in_progress:
            end_time = datetime.strptime(end_match.group(1), "%Y-%m-%d %H:%M:%S")
            text = end_match.group(2)
            duration = (end_time - current_ocr_start_time).total_seconds()

            # Store durations
            ocr_durations[current_ocr_image_id].append(duration)
            if len(text.split()) >= 2:
                ocr_durations_with_criteria[current_ocr_image_id].append({
                    "duration": duration,
                    "text": text,
                    "start_time": current_ocr_start_time,
                    "end_time": end_time
                })
            ocr_in_progress = False
            current_ocr_start_time = None

        # Detect login => start new session
        login_match = login_pattern.search(line)
        if login_match:
            # If there's an active session, close it out
            if current_session:
                second_last_ts = None
                last_ts = None
                # Move upward to find the last valid timestamps
                for j in range(i-1, -1, -1):
                    if "- config - INFO - Logging initialized for user:" in lines[j]:
                        continue
                    try:
                        ts_str = lines[j][:19]
                        t_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                        if last_ts is None:
                            last_ts = t_obj
                        elif second_last_ts is None:
                            second_last_ts = t_obj
                            break
                    except:
                        continue
                end_ts = second_last_ts if second_last_ts else last_ts
                if end_ts:
                    dur_secs = (end_ts - current_session["start_time"]).total_seconds()
                    current_session["end_time"] = end_ts
                    current_session["duration_minutes"] = round(dur_secs / 60, 2)
                    current_session["duration_seconds"] = dur_secs

                # Tally final records
                current_session["total_record_count"] = 0
                for img_id, rnums in current_session["image_records"].items():
                    current_session["total_record_count"] += max(rnums) if rnums else 0

                if not current_session["images_processed_count"]:
                    current_session["images_processed_count"] = sum(
                        1 for rnums in current_session["image_records"].values() if rnums
                    )
                sessions.append(current_session)

            # Start new
            current_user = login_match.group(2)
            current_date = login_match.group(3)
            ts = datetime.strptime(login_match.group(1), "%Y-%m-%d %H:%M:%S")
            current_session = {
                "user": current_user,
                "date": current_date,
                "start_time": ts,
                "end_time": None,
                "duration_minutes": 0,
                "duration_seconds": 0,
                "records": set(),
                "image_records": {},
                "update_count": 0,
                "character_count": 0,
                "column_edits": {},
                "images_processed_count": 0,
                "image_sections": {},
                "total_ocr_duration": 0,
                "total_name_ocr_duration": 0,
                "log_file": os.path.basename(log_file_path)
            }

        # For "sheet5" style data
        img_match_s5 = image_pattern_sheet5.search(line)
        rec_match_s5 = record_pattern_sheet5.search(line)
        if img_match_s5:
            current_image = img_match_s5.group(1)
        elif rec_match_s5 and current_image:
            rec_no = rec_match_s5.group(1)
            image_record_map[current_image].add(rec_no)

        # Check for shortcuts
        sc_match = shortcut_pattern.search(line)
        if sc_match:
            sc_value = sc_match.group(1)
            shortcuts[sc_value] = shortcuts.get(sc_value, 0) + 1

        # Additional OCR
        ocr_img_match = ocr_image_pattern.search(line)
        if ocr_img_match:
            image_num = ocr_img_match.group(1)
            image_id = ocr_img_match.group(3)
            current_ocr_image_id = image_id
            if image_id not in ocr_records:
                ocr_records[image_id] = {
                    "image_number": f"{image_num}_{ocr_img_match.group(2)}",
                    "clipboard_count": 0,
                    "name_clipboard_count": 0,
                    "user": current_user,
                    "date": current_date
                }

        # Clipboard
        clip_match = text_clipboard_pattern.search(line)
        if clip_match and current_ocr_image_id:
            ocr_records[current_ocr_image_id]["clipboard_count"] += 1
            if len(clip_match.group(1).split()) >= 2:
                ocr_records[current_ocr_image_id]["name_clipboard_count"] += 1

        # Image updated
        img_match = image_update_pattern.search(line)
        if img_match and current_session:
            img_num = img_match.group(1)
            rec_id = img_match.group(2)
            current_session["records"].add(rec_id)
            if rec_id not in current_session["image_records"]:
                current_session["image_records"][rec_id] = set()
            current_session["image_sections"][rec_id] = {
                "image_num": img_num,
                "records_processed": 0
            }

        # doc_type updated => sets images_processed_count
        doc_type_match = doc_type_update_pattern.search(line)
        if doc_type_match and current_session:
            rec_ct = int(doc_type_match.group(1))
            current_session["images_processed_count"] = rec_ct

        # r_num updated => track record
        rnum_match = r_num_pattern.search(line)
        if rnum_match and current_session:
            r_val = int(rnum_match.group(1))
            rec_id = rnum_match.group(2)
            if rec_id not in current_session["image_records"]:
                current_session["image_records"][rec_id] = set()
            current_session["image_records"][rec_id].add(r_val)
            if rec_id in current_session["image_sections"]:
                cur_section = current_session["image_sections"][rec_id]
                cur_section["records_processed"] = max(cur_section["records_processed"], r_val)

        # Field update => track field counts by user/date
        field_update_match = field_update_pattern.search(line)
        if field_update_match and current_user and current_date:
            f_name = field_update_match.group(1)
            field_updates[current_user][current_date][f_name] += 1

        # Edits => track char count
        ed_match = edit_pattern.search(line)
        if ed_match and current_session:
            col = ed_match.group(1)
            new_val = ed_match.group(2)
            rec_id = ed_match.group(3)
            current_session["character_count"] += len(new_val)
            if col not in current_session["column_edits"]:
                current_session["column_edits"][col] = 0
            current_session["column_edits"][col] += 1

        # Any update => increment
        if current_session and any_update_pattern.search(line):
            current_session["update_count"] += 1

    # Close out the final session if it exists
    if current_session:
        second_last_ts = None
        last_ts = None
        for j in range(len(lines) - 1, -1, -1):
            if "- config - INFO - Logging initialized for user:" in lines[j]:
                continue
            try:
                ts_str = lines[j][:19]
                t_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                if last_ts is None:
                    last_ts = t_obj
                elif second_last_ts is None:
                    second_last_ts = t_obj
                    break
            except:
                continue
        end_ts = second_last_ts if second_last_ts else last_ts
        if end_ts:
            dur_secs = (end_ts - current_session["start_time"]).total_seconds()
            current_session["end_time"] = end_ts
            current_session["duration_minutes"] = round(dur_secs / 60, 2)
            current_session["duration_seconds"] = dur_secs

        current_session["total_record_count"] = 0
        for img_id, rnums in current_session["image_records"].items():
            if rnums:
                current_session["total_record_count"] += max(rnums)

        if not current_session["images_processed_count"]:
            current_session["images_processed_count"] = sum(
                1 for rnums in current_session["image_records"].values() if rnums
            )
        sessions.append(current_session)

    # Convert OCR data => list of dicts
    ocr_data = []
    total_ocr_duration = 0
    total_name_ocr_duration = 0

    for image_id, info in ocr_records.items():
        durations = ocr_durations.get(image_id, [])
        total_dur = sum(durations)
        avg_dur = sum(durations) / len(durations) if durations else 0

        total_ocr_duration += total_dur

        name_dur_items = ocr_durations_with_criteria.get(image_id, [])
        name_total_dur = sum(x['duration'] for x in name_dur_items)
        total_name_ocr_duration += name_total_dur

        # If we have "name" attempts
        if name_dur_items:
            for item in name_dur_items:
                ocr_data.append({
                    "User": info["user"],
                    "Date": info["date"],
                    "Image ID": image_id,
                    "Image Number": info["image_number"],
                    "OCR Attempt": info["clipboard_count"],
                    "OCR Duration": round(item["duration"], 2),
                    "Total OCR Duration": round(item["duration"], 2),
                    "Start Time": item["start_time"].strftime("%H:%M:%S"),
                    "End Time": item["end_time"].strftime("%H:%M:%S"),
                    "Extracted Text": item["text"],
                    "Is Name OCR": "Yes",
                    "Log File": os.path.basename(log_file_path)
                })
        else:
            # Single row summarizing avg
            ocr_data.append({
                "User": info["user"],
                "Date": info["date"],
                "Image ID": image_id,
                "Image Number": info["image_number"],
                "OCR Attempt": info["clipboard_count"],
                "OCR Duration": round(avg_dur, 2),
                "Total OCR Duration": round(total_dur, 2),
                "Start Time": "",
                "End Time": "",
                "Extracted Text": "",
                "Is Name OCR": "No",
                "Log File": os.path.basename(log_file_path)
            })

    # Convert shortcuts
    shortcut_data = []
    for k, v in shortcuts.items():
        shortcut_data.append({
            "User": current_user,
            "Date": current_date,
            "SHORTCUT_NAME": k,
            "SHORTCUT": v,
            "Log File": os.path.basename(log_file_path)
        })

    # Convert image_record_map
    image_record_data = []
    for img, recs in image_record_map.items():
        image_record_data.append({
            "User": current_user,
            "Date": current_date,
            "Image Processed": img,
            "Records Processed (Unique Count)": len(recs),
            "Log File": os.path.basename(log_file_path)
        })

    # Convert field_updates => list
    field_updates_data = []
    for user_val, dates_map in field_updates.items():
        for dt_val, fields_map in dates_map.items():
            for fname, cnt in fields_map.items():
                field_updates_data.append({
                    "User": user_val,
                    "Date": dt_val,
                    "Updated Field": fname,
                    "Count": cnt,
                    "Log File": os.path.basename(log_file_path)
                })

    # Attach total OCR durations
    for s in sessions:
        s["total_ocr_duration"] = total_ocr_duration
        s["total_name_ocr_duration"] = total_name_ocr_duration

    return sessions, ocr_data, shortcut_data, image_record_data, field_updates_data

############################################################
## analyze_time_gaps
############################################################

def analyze_time_gaps(log_file_path):
    """
    Analyze time gaps >= 2 minutes between consecutive lines in a log file.
    """
    gaps = []
    login_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (?:config) - INFO - Logging initialized for user: (.+) on (\d{4}-\d{2}-\d{2})"
    )

    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading log file {log_file_path}: {e}")
        return gaps

    current_user = None
    current_date = None

    for i in range(len(lines) - 1):
        login_match = login_pattern.search(lines[i])
        if login_match:
            current_user = login_match.group(2)
            current_date = login_match.group(3)
            continue
        
        try:
            ts_now, text_now = extract_timestamp_line(lines[i])
            ts_next, text_next = extract_timestamp_line(lines[i+1])
            if ts_now and ts_next:
                delta = ts_next - ts_now
                delta_mins = delta.total_seconds() / 60
                if delta_mins >= 2:
                    gaps.append({
                        "User": current_user,
                        "Date": current_date,
                        "Start Time": ts_now.strftime("%H:%M:%S"),
                        "End Time": ts_next.strftime("%H:%M:%S"),
                        "Duration": format_time_duration(int(delta.total_seconds())),
                        "Duration (minutes)": round(delta_mins, 2),
                        "Start Line": text_now,
                        "End Line": text_next,
                        "Log File": os.path.basename(log_file_path)
                    })
        except:
            continue
    return gaps

############################################################
## extract_detailed_ocr_data
############################################################

def extract_detailed_ocr_data(log_file_path):
    """
    Additional approach for scanning lines like 'perform_ocr_on_cropped_image:' etc.
    Returns detailed OCR data in a list of dicts.
    """
    results = []
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading file {log_file_path} in extract_detailed_ocr_data: {e}")
        return results

    fn = os.path.basename(log_file_path)
    user_from_filename = "Unknown"
    date_from_filename = "Unknown"

    user_match = re.search(r'(\d+)_', fn)
    if user_match:
        user_from_filename = user_match.group(1)

    date_match = re.search(r'_(\d{4}-\d{2}-\d{2})\.log', fn)
    if date_match:
        date_from_filename = date_match.group(1)

    login_pattern = re.compile(r"Logging initialized for user: ([^\.]+)\.? on (\d{4}-\d{2}-\d{2})")

    current_user = user_from_filename
    current_date = date_from_filename
    current_ocr = None

    for line in lines:
        udm = login_pattern.search(line)
        if udm:
            current_user = udm.group(1).strip()
            current_date = udm.group(2)

        if "perform_ocr_on_cropped_image:" in line:
            # Wrap up the previous if needed
            if current_ocr and current_ocr.get("start_time"):
                if not current_ocr.get("end_time") and current_ocr.get("clipboard_time"):
                    current_ocr["end_time"] = current_ocr["clipboard_time"]
                if current_ocr["start_time"] and current_ocr["end_time"]:
                    secs = (current_ocr["end_time"] - current_ocr["start_time"]).total_seconds()
                    if secs < 0: 
                        secs = 0
                    results.append({
                        "User": current_user,
                        "Date": current_date,
                        "Start Time": current_ocr["start_time"].strftime("%H:%M:%S"),
                        "End Time": current_ocr["end_time"].strftime("%H:%M:%S") if current_ocr["end_time"] else "",
                        "Duration (seconds)": secs,
                        "Duration (minutes)": secs / 60,
                        "Original Text": current_ocr.get("original_text", ""),
                        "Clipboard Text": current_ocr.get("clipboard_text", ""),
                        "Has UPDATED": current_ocr.get("has_updated", False),
                        "Log File": fn
                    })
            # Start new
            ts_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if ts_match:
                st_time = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
                current_ocr = {
                    "start_time": st_time,
                    "end_time": None,
                    "original_text": "",
                    "clipboard_text": "",
                    "clipboard_time": None,
                    "has_updated": False
                }
            else:
                current_ocr = None

        elif "Original Text =>" in line and current_ocr:
            tmatch = re.search(r"Original Text => '([^']*)'", line)
            if tmatch:
                current_ocr["original_text"] = tmatch.group(1)

        elif "Text copied to clipboard:" in line and current_ocr:
            tmatch = re.search(r"Text copied to clipboard: '([^']*)'", line)
            if tmatch:
                current_ocr["clipboard_text"] = tmatch.group(1)
            ts_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if ts_match:
                c_time = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
                current_ocr["clipboard_time"] = c_time
                if not current_ocr["end_time"]:
                    current_ocr["end_time"] = c_time

        elif "UPDATED" in line and current_ocr and current_ocr.get("clipboard_text"):
            # If at least some overlap in text
            partials = current_ocr["clipboard_text"].split()[:3]
            if any(p in line for p in partials if len(p) > 2):
                current_ocr["has_updated"] = True
                ts_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if ts_match:
                    e_time = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
                    current_ocr["end_time"] = e_time

    # Wrap up the last OCR
    if current_ocr and current_ocr.get("start_time"):
        if not current_ocr.get("end_time") and current_ocr.get("clipboard_time"):
            current_ocr["end_time"] = current_ocr["clipboard_time"]
        if current_ocr["start_time"] and current_ocr["end_time"]:
            secs = (current_ocr["end_time"] - current_ocr["start_time"]).total_seconds()
            if secs < 0:
                secs = 0
            results.append({
                "User": current_user,
                "Date": current_date,
                "Start Time": current_ocr["start_time"].strftime("%H:%M:%S"),
                "End Time": current_ocr["end_time"].strftime("%H:%M:%S") if current_ocr["end_time"] else "",
                "Duration (seconds)": secs,
                "Duration (minutes)": secs / 60,
                "Original Text": current_ocr.get("original_text", ""),
                "Clipboard Text": current_ocr.get("clipboard_text", ""),
                "Has UPDATED": current_ocr.get("has_updated", False),
                "Log File": fn
            })

    return results

############################################################
## extract_ocr_durations_for_new_sheet
############################################################

def extract_ocr_durations_for_new_sheet(folder_path):
    """
    Recursively searches for .log files in folder_path to build two DataFrames:
      df_ocr_data and df_ocr_summary
    """
    log_files = glob.glob(os.path.join(folder_path, "**", "*.log"), recursive=True)
    if not log_files:
        print(f"No log files found in: {folder_path} for new OCR snippet.")
        return None, None

    print(f"Found {len(log_files)} log files to process for new OCR snippet.")
    all_ocr_data = []

    for lf in log_files:
        base = os.path.basename(lf)

        # Attempt to parse user/date from filename
        user_id = "Unknown"
        dt_str = "Unknown"

        m_user = re.search(r'(\d+)_', base)
        if m_user:
            user_id = m_user.group(1)

        m_dt = re.search(r'_(\d{4}-\d{2}-\d{2})\.log', base)
        if m_dt:
            dt_str = m_dt.group(1)

        try:
            with open(lf, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
        except Exception as e:
            print(f"Error reading file {lf}: {e}")
            continue

        file_ocr_data = []
        current_ocr = None

        for line in lines:
            if "perform_ocr_on_cropped_image:" in line:
                # Close out the previous
                if current_ocr and current_ocr.get("start_time"):
                    if not current_ocr.get("end_time") and current_ocr.get("clipboard_time"):
                        current_ocr["end_time"] = current_ocr["clipboard_time"]
                    if current_ocr["start_time"] and current_ocr["end_time"]:
                        dur_s = (current_ocr["end_time"] - current_ocr["start_time"]).total_seconds()
                        if dur_s < 0: 
                            dur_s = 0
                        file_ocr_data.append({
                            "user": user_id,
                            "date": dt_str,
                            "start_time": current_ocr["start_time"].strftime("%H:%M:%S"),
                            "end_time": current_ocr["end_time"].strftime("%H:%M:%S"),
                            "duration_seconds": dur_s,
                            "duration_minutes": dur_s / 60,
                            "original_text": current_ocr.get("original_text", "").replace("\n", " "),
                            "clipboard_text": current_ocr.get("clipboard_text", "").replace("\n", " "),
                            "is_instant": dur_s == 0
                        })
                # Start a new OCR
                ts_m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if ts_m:
                    current_ocr = {
                        "start_time": datetime.strptime(ts_m.group(1), "%Y-%m-%d %H:%M:%S"),
                        "end_time": None,
                        "original_text": "",
                        "clipboard_text": "",
                        "clipboard_time": None
                    }
                else:
                    current_ocr = None

            elif "Original Text =>" in line and current_ocr:
                txt_m = re.search(r"Original Text => '([^']*)'", line)
                if txt_m:
                    current_ocr["original_text"] = txt_m.group(1)

            elif "Text copied to clipboard:" in line and current_ocr:
                txt_m = re.search(r"Text copied to clipboard: '([^']*)'", line)
                if txt_m:
                    current_ocr["clipboard_text"] = txt_m.group(1)
                ts_m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if ts_m:
                    current_ocr["clipboard_time"] = datetime.strptime(ts_m.group(1), "%Y-%m-%d %H:%M:%S")
                    if not current_ocr["end_time"]:
                        current_ocr["end_time"] = current_ocr["clipboard_time"]

            elif "UPDATED" in line and current_ocr and current_ocr.get("clipboard_text"):
                c_txt = current_ocr["clipboard_text"].replace("\n", " ").strip()
                # If the line has at least 1 matching chunk from the first 3 words
                if any(part in line for part in c_txt.split()[:3] if len(part) > 2):
                    ts_m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                    if ts_m:
                        current_ocr["end_time"] = datetime.strptime(ts_m.group(1), "%Y-%m-%d %H:%M:%S")

        # End of file => close out the last one
        if current_ocr and current_ocr.get("start_time"):
            if not current_ocr.get("end_time") and current_ocr.get("clipboard_time"):
                current_ocr["end_time"] = current_ocr["clipboard_time"]
            if current_ocr["start_time"] and current_ocr["end_time"]:
                dur_s = (current_ocr["end_time"] - current_ocr["start_time"]).total_seconds()
                if dur_s < 0:
                    dur_s = 0
                file_ocr_data.append({
                    "user": user_id,
                    "date": dt_str,
                    "start_time": current_ocr["start_time"].strftime("%H:%M:%S"),
                    "end_time": current_ocr["end_time"].strftime("%H:%M:%S"),
                    "duration_seconds": dur_s,
                    "duration_minutes": dur_s / 60,
                    "original_text": current_ocr.get("original_text", "").replace("\n", " "),
                    "clipboard_text": current_ocr.get("clipboard_text", "").replace("\n", " "),
                    "is_instant": dur_s == 0
                })

        all_ocr_data.extend(file_ocr_data)

    df_all_ocr = pd.DataFrame(all_ocr_data)
    if df_all_ocr.empty:
        print("No OCR data found in snippet logs.")
        return None, None

    # "OCR Data" sheet
    df_ocr_data = df_all_ocr[[
        "user", "date", "start_time", "end_time", "duration_seconds", "duration_minutes"
    ]].copy()

    # Summaries => "OCR Summary"
    summary_rows = []
    for (u, d), grp in df_ocr_data.groupby(["user", "date"]):
        total_ops = len(grp)
        inst_ops = len(grp[grp["duration_seconds"] == 0])
        total_minutes = grp["duration_minutes"].sum()

        summary_rows.append({
            "user": u,
            "date": d,
            "total OCR duration": total_minutes,
            "Total OCR attempt": total_ops,
            "Partially OCR Attempt": inst_ops,
            "OCR Attempt": total_ops - inst_ops
        })
    df_ocr_summary = pd.DataFrame(summary_rows)

    # Convert numeric minutes => HH:MM:SS string
    df_ocr_data["duration_as_HHMMSS"] = df_ocr_data["duration_minutes"].apply(
        lambda x: str(timedelta(minutes=x)).split(".")[0]
    )
    df_ocr_summary["total OCR duration_formatted"] = df_ocr_summary["total OCR duration"].apply(
        lambda x: str(timedelta(minutes=x)).split(".")[0]
    )

    return df_ocr_data, df_ocr_summary

############################################################
## Additional helpers to read user ID / date and parse images
############################################################

def extract_user_id(filename):
    """
    Extracts the user ID from the filename using a regex.
    Returns the first numeric substring, or 'Unknown'.
    """
    match = re.search(r"(\d+)", filename)
    return match.group(1) if match else "Unknown"

def extract_date_from_filename(filename):
    """
    Attempt to extract date (YYYY-MM-DD) from filename, e.g. '123_2023-05-01.log'.
    Returns 'Unknown' if not found.
    """
    match = re.search(r"_(\d{4}-\d{2}-\d{2})\.log", filename)
    return match.group(1) if match else "Unknown"

def analyze_user_images_in_file(log_file_path):
    """
    Parse a single log file to figure out:
      - user ID (from filename)
      - date (from filename)
      - unique image count
      - processed image count
    Returns (user_id, date_str, unique_count, processed_count).
    """
    fn = os.path.basename(log_file_path)
    user_id = extract_user_id(fn)
    date_str = extract_date_from_filename(fn)

    unique_imgs = set()
    processed_imgs = set()
    last_img = None

    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if "Updated IMAGE_NUMBER to" in line:
                    parts = line.strip().split()
                    image_info = parts[-1]
                    unique_imgs.add(image_info)
                    last_img = image_info
                elif "UPDATED" in line and last_img:
                    processed_imgs.add(last_img)
    except Exception as e:
        print(f"Error reading {log_file_path} for user-images: {e}")

    return user_id, date_str, len(unique_imgs), len(processed_imgs)

############################################################
## collect_updated_fields_snippet
############################################################

def collect_updated_fields_snippet(folder_path):
    """
    Uses snippet logic to read .log files, detect 'UPDATED <FIELD>',
    and store them by (User, Date, Updated_Field). Returns a DataFrame.
    """
    # Regex for "Logging initialized for user: <username> on YYYY-MM-DD"
    user_date_pattern = re.compile(r'.*Logging initialized for user: ([^\.]+)\.? on (\d{4}-\d{2}-\d{2})')
    # Pattern for "UPDATED <FIELD>"
    update_pattern = re.compile(r'\bUPDATED\b\s+(\S+)')

    updates_list = []
    processed_files = 0

    for filename in os.listdir(folder_path):
        if filename.lower().endswith('.log'):
            file_path = os.path.join(folder_path, filename)

            try:
                current_user = "Unknown"
                current_date = "Unknown"

                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    for line in file:
                        user_date_match = user_date_pattern.search(line)
                        if user_date_match:
                            current_user = user_date_match.group(1).strip()
                            current_date = user_date_match.group(2)

                        update_match = update_pattern.search(line)
                        if update_match:
                            field_name = update_match.group(1)
                            updates_list.append({
                                'User': current_user,
                                'Date': current_date,
                                'Updated Field': field_name,
                                'Count': 1
                            })

                processed_files += 1
            except Exception as e:
                print(f"❌ Error processing file {filename}: {e}")

    if processed_files == 0:
        print("⚠️ No .log files found in the specified folder (snippet).")
        return pd.DataFrame()

    if not updates_list:
        print("⚠️ No 'UPDATED' entries found in the log files (snippet).")
        return pd.DataFrame()

    # Convert list => DataFrame
    df = pd.DataFrame(updates_list)
    return df

############################################################
## create_excel_report
############################################################

def create_excel_report(
    all_sessions, all_ocr_data, all_shortcut_data,
    all_image_record_data, all_time_gaps, all_break_times,
    all_updated_fields, all_detailed_ocr_data,
    output_path,
    df_ocr_data=None,
    df_ocr_summary=None,
    df_user_images=None,
    df_updated_fields_snippet=None  # <-- from snippet
):
    """
    Creates the final Excel report with multiple sheets, including:
      - Session Summary
      - Duration & OCR Summary
      - Shortcut Analysis
      - Image Record Processing
      - Time Gaps Analysis
      - Break Times
      - Updated Fields Pivot (generated from snippet approach)
      - OCR Data + OCR Summary (if provided)
      - User Image Summary (if provided)
    """
    print(f"Creating Excel report at: {output_path}")

    ################################################################
    # 1) Session Summary
    ################################################################
    row_list = []
    for sess in all_sessions:
        row_list.append({
            "User": sess["user"],
            "Date": sess["date"],
            "Start Time": sess["start_time"].strftime("%H:%M:%S"),
            "End Time": sess["end_time"].strftime("%H:%M:%S") if sess["end_time"] else "",
            "Duration (minutes)": sess["duration_minutes"],
            "Total Images": len(sess["records"]),
            "Update Count": sess["update_count"],
            "Character Count": sess["character_count"],
            "Log File": sess["log_file"]
        })
    df_sessions = pd.DataFrame(row_list)

    ################################################################
    # 2) Duration & OCR Summary (per-user-date aggregates)
    ################################################################
    grouped = defaultdict(list)
    for s in all_sessions:
        grouped[(s["user"], s["date"])].append(s)

    # Summation of idle time from all_time_gaps => (user, date)
    user_date_idle_minutes = defaultdict(float)
    for gap in all_time_gaps:
        user_date_idle_minutes[(gap["User"], gap["Date"])] += gap["Duration (minutes)"]

    # Summation of break times => (user, date)
    user_date_break_seconds = defaultdict(int)
    for bk in all_break_times:
        user_date_break_seconds[(bk["User"], bk["Date"])] += bk["Break in seconds"]

    summary_rows = []
    total_duration_secs = 0
    total_idle_mins = 0
    total_break_secs = 0
    total_char_count = 0
    total_updates = 0
    total_shortcuts = 0
    total_records_processed = 0
    log_files_all = set()

    for (user, dt), s_list in grouped.items():
        # sum durations
        dur_s = sum(x["duration_seconds"] for x in s_list)
        total_duration_secs += dur_s

        # idle => convert to seconds
        idle_m = user_date_idle_minutes.get((user, dt), 0)
        total_idle_mins += idle_m
        idle_s = int(idle_m * 60)

        # break => seconds
        bk_s = user_date_break_seconds.get((user, dt), 0)
        total_break_secs += bk_s

        # actual idle
        actual_idle_s = max(0, idle_s - bk_s)

        # char count + updates
        c_count = sum(x["character_count"] for x in s_list)
        total_char_count += c_count
        upd_count = sum(x["update_count"] for x in s_list)
        total_updates += upd_count

        # shortcuts for this user/date
        sc_count = sum(
            sc["SHORTCUT"] for sc in all_shortcut_data
            if sc["User"] == user and sc["Date"] == dt
        )
        total_shortcuts += sc_count

        # records processed
        rec_proc = 0
        relevant_imgs = [i for i in all_image_record_data if i["User"] == user and i["Date"] == dt]
        for item in relevant_imgs:
            rec_proc += item["Records Processed (Unique Count)"]
        total_records_processed += rec_proc

        # gather log files
        for ss in s_list:
            log_files_all.add(ss["log_file"])

        # Build the row
        summary_rows.append({
            "User": user,
            "Date": dt,
            "Total Duration": format_time_duration(dur_s),
            "Total Ideal Time": format_time_duration(idle_s),
            "Total Break Time": format_time_duration(bk_s),
            "Actual Ideal Time": format_time_duration(actual_idle_s),
            "Total Break Seconds": bk_s,
            "Total Shortcuts": sc_count,
            "Total Character Count": c_count,
            "Total Records Processed": rec_proc,
            "Total Field Edits": upd_count,
            "Log Files Processed": len({ss["log_file"] for ss in s_list})
        })

    # Grand total row
    total_actual_idle_secs = max(0, int(total_idle_mins * 60) - total_break_secs)
    summary_rows.append({
        "User": "Total (All Users)",
        "Date": "All Dates",
        "Total Duration": format_time_duration(total_duration_secs),
        "Total Ideal Time": format_time_duration(int(total_idle_mins * 60)),
        "Total Break Time": format_time_duration(total_break_secs),
        "Actual Ideal Time": format_time_duration(total_actual_idle_secs),
        "Total Break Seconds": total_break_secs,
        "Total Shortcuts": total_shortcuts,
        "Total Character Count": total_char_count,
        "Total Records Processed": total_records_processed,
        "Total Field Edits": total_updates,
        "Log Files Processed": len(log_files_all)
    })

    df_summary = pd.DataFrame(summary_rows)

    # Merge in "User Image Summary" if present
    if df_user_images is not None and not df_user_images.empty:
        df_summary = pd.merge(
            df_summary,
            df_user_images[["User ID", "Date", "Total Image Count", "Processed Image Count"]].rename(
                columns={"User ID": "User"}
            ),
            on=["User", "Date"],
            how="left"
        )
        df_summary["Total Image Count"] = df_summary["Total Image Count"].fillna(0).astype(int)
        df_summary["Processed Image Count"] = df_summary["Processed Image Count"].fillna(0).astype(int)

    ################################################################
    # 3) Build all DataFrames
    ################################################################
    df_shortcuts = pd.DataFrame(all_shortcut_data)
    df_image_records = pd.DataFrame(all_image_record_data)
    df_time_gaps_df = pd.DataFrame(all_time_gaps)
    df_break_times_df = pd.DataFrame(all_break_times)
    df_updated_fields_df = pd.DataFrame(all_updated_fields)

    # Restrict break times columns
    if not df_break_times_df.empty:
        df_break_times_df = df_break_times_df[
            ["User", "Date", "Starting", "Ending", "Break Time"]
        ].rename(columns={
            "Starting": "Start Time",
            "Ending": "End Time"
        })

    # If we also have the snippet-based updated fields
    df_updated_fields_pivot = None
    if df_updated_fields_snippet is not None and not df_updated_fields_snippet.empty:
        # Create pivot => [User, Date], columns=Updated Field
        pivot_df = pd.pivot_table(
            df_updated_fields_snippet,
            index=["User", "Date"],
            columns="Updated Field",
            values="Count",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
        df_updated_fields_pivot = pivot_df

    # Unique output path
    if output_path.endswith(".xlsx"):
        base_path = output_path[:-5]
        ext = ".xlsx"
    else:
        base_path = output_path
        ext = ""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_path = f"{base_path}_{stamp}{ext}"

    try:
        with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
            # 1) Session Summary
            df_sessions.to_excel(writer, sheet_name="Session Summary", index=False)
            # 2) Duration and OCR Summary
            df_summary.to_excel(writer, sheet_name="Duration and OCR Summary", index=False)
            # 3) Shortcut Analysis
            df_shortcuts.to_excel(writer, sheet_name="Shortcut Analysis", index=False)
            # 4) Image Record Processing
            df_image_records.to_excel(writer, sheet_name="Image Record Processing", index=False)
            # 5) Time Gaps Analysis
            df_time_gaps_df.to_excel(writer, sheet_name="Time Gaps Analysis", index=False)
            # 6) Break Times
            df_break_times_df.to_excel(writer, sheet_name="Break Times", index=False)

            # 7) Updated Fields Pivot (snippet-based)
            if df_updated_fields_pivot is not None and not df_updated_fields_pivot.empty:
                df_updated_fields_pivot.to_excel(writer, sheet_name="Updated Fields Pivot", index=False)

            # 8) OCR Data + OCR Summary
            if df_ocr_data is not None and not df_ocr_data.empty:
                df_ocr_data.to_excel(writer, sheet_name="OCR Data", index=False)
            if df_ocr_summary is not None and not df_ocr_summary.empty:
                df_ocr_summary.to_excel(writer, sheet_name="OCR Summary", index=False)

            # 9) We can also store df_updated_fields_df (the base code’s field updates) if desired
            if not df_updated_fields_df.empty:
                df_updated_fields_df.to_excel(writer, sheet_name="Updated Fields (Base Code)", index=False)

            # 10) User Image Summary
            if df_user_images is not None and not df_user_images.empty:
                df_user_images.to_excel(writer, sheet_name="User Image Summary", index=False)

        print(f"Excel report generated successfully: {final_path}")
        return final_path
    except Exception as e:
        print(f"Error writing Excel file: {e}")
        raise

############################################################
## process_log_folder
############################################################

def process_log_folder(folder_path, output_excel_path):
    """
    Process .log files in the specified folder => single Excel report.
    Incorporates:
      - Multi-sheet base analysis
      - Additional OCR snippet data (df_ocr_data, df_ocr_summary)
      - "User Image Summary" (unique & processed counts)
      - "Break Times"
      - "Updated Fields Pivot" (from snippet code)
    """
    print(f"Processing all log files in folder: {folder_path}")

    all_sessions = []
    all_ocr_data = []
    all_shortcut_data = []
    all_image_record_data = []
    all_time_gaps = []
    all_updated_fields = []
    all_detailed_ocr_data = []

    # Track user image stats
    user_image_map = defaultdict(lambda: {"Unique": 0, "Processed": 0})

    log_count = 0
    for fn in os.listdir(folder_path):
        if fn.lower().endswith(".log"):
            log_count += 1
            full_path = os.path.join(folder_path, fn)
            print(f"Processing log file {log_count}: {fn}")
            try:
                sess, ocr, sc, img_rec, f_updates = analyze_log_file(full_path)
                tgaps = analyze_time_gaps(full_path)
                det_ocr = extract_detailed_ocr_data(full_path)

                # Accumulate session data
                all_sessions.extend(sess)
                all_ocr_data.extend(ocr)
                all_shortcut_data.extend(sc)
                all_image_record_data.extend(img_rec)
                all_time_gaps.extend(tgaps)
                all_detailed_ocr_data.extend(det_ocr)

                # Merge updated_fields from analyze_log_file
                all_updated_fields.extend(f_updates)

                # user image summary
                uid, dval, uniq, proc = analyze_user_images_in_file(full_path)
                user_image_map[(uid, dval)]["Unique"] += uniq
                user_image_map[(uid, dval)]["Processed"] += proc

            except Exception as e:
                print(f"Error processing log file {fn}: {e}")

    if log_count == 0:
        print("No log files found in the specified folder.")
        return None

    # Compute break times across sessions
    all_break_times = calculate_break_times(all_sessions)

    print(f"Completed processing {log_count} log files")
    print(
        "Total data collected: "
        f"{len(all_sessions)} sessions, "
        f"{len(all_ocr_data)} OCR records, "
        f"{len(all_shortcut_data)} shortcuts, "
        f"{len(all_time_gaps)} time gaps, "
        f"{len(all_break_times)} break periods, "
        f"{len(all_updated_fields)} field updates, "
        f"{len(all_detailed_ocr_data)} new-detailed OCR records"
    )

    # Generate snippet-based OCR DataFrames
    df_ocr_data, df_ocr_summary = extract_ocr_durations_for_new_sheet(folder_path)

    # Build user-images DataFrame
    rows_ui = []
    for (u, d), counts in user_image_map.items():
        rows_ui.append({
            "User ID": u,
            "Date": d,
            "Total Image Count": counts["Unique"],
            "Processed Image Count": counts["Processed"]
        })
    df_user_images = pd.DataFrame(rows_ui)

    # Also collect snippet-based updated fields
    df_snippet_updated = collect_updated_fields_snippet(folder_path)
    # (We do not pivot here; we’ll pivot inside create_excel_report)

    # If we have at least some data, create the Excel
    if any([
        all_sessions,
        all_ocr_data,
        all_shortcut_data,
        all_time_gaps,
        all_break_times,
        all_updated_fields,
        all_detailed_ocr_data
    ]):
        return create_excel_report(
            all_sessions=all_sessions,
            all_ocr_data=all_ocr_data,
            all_shortcut_data=all_shortcut_data,
            all_image_record_data=all_image_record_data,
            all_time_gaps=all_time_gaps,
            all_break_times=all_break_times,
            all_updated_fields=all_updated_fields,  # base code’s updates
            all_detailed_ocr_data=all_detailed_ocr_data,
            output_path=output_excel_path,
            df_ocr_data=df_ocr_data,
            df_ocr_summary=df_ocr_summary,
            df_user_images=df_user_images,
            df_updated_fields_snippet=df_snippet_updated  # snippet approach
        )
    else:
        print("No data found to analyze.")
        return None

############################################################
## Main
############################################################

if __name__ == "__main__":
    folder_path = r"C:\Users\11884\Desktop\Demo"  # Change as needed
    output_excel_path = r"C:\Users\11884\Desktop\Demo\record.xlsx"

    print("Starting log analysis with multi-sheet + snippet-based updated fields pivot.")
    final_path = process_log_folder(folder_path, output_excel_path)
    if final_path:
        print(f"Unified multi-sheet report created: {final_path}")
    else:
        print("No data found for analysis.")
