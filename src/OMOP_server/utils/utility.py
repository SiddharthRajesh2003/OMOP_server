import datetime
def map_gender(gender):
    if not gender:
        return 0
    
    gender_clean = str(gender).strip().lower()
    if gender_clean == 'male':
        return 8507
    elif gender_clean in ['female', 'f']:
        return 8532
    return 0

def map_race(race):
    if not race:
        return 0
    
    race_clean = str(race).strip().lower()
    
    if race_clean in ['black or african american', 'black', 'african american']:
        return 8516
    elif race_clean in ['american indian or alaska native','American Indian or Alaska Native', 'american indian','American Indian', 'alaska native','Alaska Native', 'native american','Native American']:
        return 8657
    elif race_clean in ['asian','Asian']:
        return 8515
    elif race_clean in ['native hawaiian or other pacific islander','Native Hawaiian or Other Pacific Islander', 'native hawaiian','Native Hawaiian', 'pacific islander','Pacific Islander']:
        return 8557
    elif race_clean in ['white', 'caucasian','White','Caucasian']:
        return 8527
    else:
        return 0

def map_ethnicity(eth):
    if not eth:
        return 0
    
    eth_clean = str(eth).strip().lower()
    
    if eth_clean in ['hispanic or latino', 'hispanic', 'latino']:
        return 38003563
    elif eth_clean in ['not hispanic or latino', 'not hispanic', 'non-hispanic']:
        return 38003564
    else:
        return 0

def sql_val(val):
    if val is None or val==' ':
        return 'NULL'
    if isinstance(val, str):
        val_escaped = val.replace("'", "''")
        return f"'{val_escaped}'"
    if isinstance(val, (datetime.datetime, datetime.date)):
        return f"'{val}'"
    return str(val)


def map_visit_type(v):
    if not v:
        return 0
    
    v_clean = str(v).strip().lower()
    
    if 'clinic' in v_clean:
        return 38004247
    elif 'emergency' in v_clean:
        return 9203
    elif 'inpatient' in v_clean:
        return 9201
    elif 'outpatient' in v_clean:
        return 9202
    elif 'phone' in v_clean:
        return 722455
    elif 'outreach' in v_clean:
        return 38004202
    elif 'research' in v_clean:
        return 38004259
    elif 'lab' in v_clean:
        return 32036
    else:
        return 0
    
def map_admit(a):
    if not a:
        return 0
    
    a_clean = str(a).strip().lower()
    
    
    if 'clinic' in a_clean:
        if 'physician' in a_clean:
            return 38004247
    elif 'emergency' in a_clean:
        return 9203
    elif 'routine' in a_clean:
        return 9201
    elif 'self' in a_clean:
        return 0
    elif 'nurs' in a_clean:
        return 8863
    elif 'lab' in a_clean:
        return 32036
    elif 'born' in a_clean:
        if 'out' in a_clean:
            return 0
        elif 'sick' in a_clean:
            return 38004444
        else:
            return 8650
    elif 'hospice' in a_clean:
        return 8546
    elif 'law' in a_clean:
        return 32761
    elif 'correctional' in a_clean:
        return 32761
    elif 'ambulatory' in a_clean:
        if 'surgery' in a_clean:
            return 8883
    elif 'acute' in a_clean:
        return 38004279
    elif 'info' in a_clean:
        if 'not' in a_clean:
            return 0
    elif 'hospital' in a_clean:
        if 'transfer' in a_clean:
            return 38004515
    elif 'outpatient' in a_clean:
        return 9202
    elif 'delivery' in a_clean:
        return 8650
    elif 'non' in a_clean:
        return 0
    elif 'transfer' in a_clean:
        if 'hc' in a_clean:
            return 38004515
    

def map_discharge(d):
    if not d:
        return 0

    d_clean = str(d).strip().lower()

    # List of keywords for hospital/facility names
    hospital_keywords = {
        'hospital', 'iu health', 'community', 'st vincent', 'st vincents', 'eskenazi', 'riley', 'methodist',
        'baptist', 'floyd', 'bedford', 'harsha', 'ball', 'neuro', 'psych', 'fayette', 'eskenazi', 'rehabilitation',
        'white', 'norton', 'columbus', 'developmental', 'children', 'blackford', 'hendricks', 'midtown', 'monroe',
        'especially kids', 'elizabeth', 'jay', 'skilled nursing', 'damar', 'valle vista', 'university', 'nursing home',
        'jasper', 'francis', 'beech grove', 'good samaritan', 'morgan', 'north', 'deaconess', 'sycamore', 'wishard',
        'west', 'rehab', 'ltac', 'arnett', 'schneck', 'clark', 'terre haute', 'frankfort', 'extended care',
        'meridian', 'va hospital', 'tipton', 'heart center', 'acute care', 'union', 'witham', 'river bend',
        'options', 'wellstone', 'wabash valley', 'meadows', 'saxony', 'paoli', 'bloomington', 'meadows', 'meadows',
        'lifesprings', 'turning pt', 'indiana heart', 'indiana', 'rahe', 'clay county', 'south', 'central', 'east'
    }

    # If any hospital keyword is in the discharge string, return the hospital concept code
    for keyword in hospital_keywords:
        if keyword in d_clean:
            return 38004515  # OMOP concept for "Transfer to another hospital"

    # Existing logic for other mappings
    if 'home' in d_clean:
        return 0
    elif 'lab' in d_clean:
        return 32036
    elif 'long' in d_clean:
        return 38004277
    elif 'nurs' in d_clean:
        return 8863
    elif 'born' in d_clean:
        if 'out' in d_clean:
            return 0
        elif 'sick' in d_clean:
            return 38004444
        else:
            return 8650
    elif 'hospice' in d_clean:
        return 8546
    elif 'law' in d_clean:
        return 32761
    elif 'correctional' in d_clean:
        return 32761
    elif 'ambulatory' in d_clean:
        if 'surgery' in d_clean:
            return 8883
    elif 'acute' in d_clean:
        return 38004279
    elif 'info' in d_clean:
        if 'not' in d_clean:
            return 0
    elif 'outpatient' in d_clean:
        return 9202
    elif 'delivery' in d_clean:
        return 8650
    elif 'non' in d_clean:
        return 0
    elif 'transfer' in d_clean:
        if 'hc' in d_clean:
            return 38004515

    return 0