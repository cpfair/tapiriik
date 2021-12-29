from tapiriik.services.interchange import ActivityType

# Activity types from:
# https://developers.google.com/fit/rest/v1/reference/activity-types
# There are lots that got lumped into "other".

googlefit_to_atype = {
    0: ActivityType.Other,    # In vehicle
    1: ActivityType.Cycling,  # Biking
    2: ActivityType.Walking,  # On foot
    3: ActivityType.Other,  # Still (not moving)
    4: ActivityType.Other,  # Unknown (unable to detect activity)
    5: ActivityType.Other,  # Tilting (sudden device gravity change)
    7: ActivityType.Walking,  # Walking
    8: ActivityType.Running,  # Running
    9: ActivityType.Gym,  # Aerobics
    10: ActivityType.Other,  # Badminton
    11: ActivityType.Other,  # Baseball
    12: ActivityType.Other,  # Basketball
    13: ActivityType.Other,  # Biathlon
    14: ActivityType.Cycling,  # Handbiking
    15: ActivityType.MountainBiking,  # Mountain biking
    16: ActivityType.Cycling,  # Road biking
    17: ActivityType.Cycling,  # Spinning
    18: ActivityType.Cycling,  # Stationary biking
    19: ActivityType.Cycling,  # Utility biking
    20: ActivityType.Other,  # Boxing
    21: ActivityType.Gym,  # Calisthenics
    22: ActivityType.Gym,  # Circuit training
    23: ActivityType.Other,  # Cricket
    24: ActivityType.Other,  # Dancing
    25: ActivityType.Elliptical,  # Elliptical
    26: ActivityType.Other,  # Fencing
    27: ActivityType.Other,  # Football (American)
    28: ActivityType.Other,  # Football (Australian)
    29: ActivityType.Other,  # Football (Soccer)
    30: ActivityType.Other,  # Frisbee
    31: ActivityType.Other,  # Gardening
    32: ActivityType.Other,  # Golf
    33: ActivityType.Gym,  # Gymnastics
    34: ActivityType.Other,  # Handball
    35: ActivityType.Hiking,  # Hiking
    36: ActivityType.Other,  # Hockey
    37: ActivityType.Other,  # Horseback riding
    38: ActivityType.Other,  # Housework
    39: ActivityType.Other,  # Jumping rope
    40: ActivityType.Other,  # Kayaking
    41: ActivityType.Gym,  # Kettlebell training
    42: ActivityType.Other,  # Kickboxing
    43: ActivityType.Other,  # Kitesurfing
    44: ActivityType.Other,  # Martial arts
    45: ActivityType.Other,  # Meditation
    46: ActivityType.Other,  # Mixed martial arts
    47: ActivityType.Other,  # P90X exercises
    48: ActivityType.Other,  # Paragliding
    49: ActivityType.Other,  # Pilates
    50: ActivityType.Other,  # Polo
    51: ActivityType.Other,  # Racquetball
    52: ActivityType.Climbing,  # Rock climbing
    53: ActivityType.Rowing,  # Rowing
    54: ActivityType.Rowing,  # Rowing machine
    55: ActivityType.Other,  # Rugby
    56: ActivityType.Running,  # Jogging
    57: ActivityType.Running,  # Running on sand
    58: ActivityType.Running,  # Running (treadmill)
    59: ActivityType.Other,  # Sailing
    60: ActivityType.Other,  # Scuba diving
    61: ActivityType.Other,  # Skateboarding
    62: ActivityType.Skating,  # Skating
    63: ActivityType.Skating,  # Cross skating
    64: ActivityType.Skating,  # Inline skating (rollerblading)
    65: ActivityType.DownhillSkiing,  # Skiing
    66: ActivityType.CrossCountrySkiing,  # Back-country skiing
    67: ActivityType.CrossCountrySkiing,  # Cross-country skiing
    68: ActivityType.DownhillSkiing,  # Downhill skiing
    69: ActivityType.DownhillSkiing,  # Kite skiing
    70: ActivityType.DownhillSkiing,  # Roller skiing
    71: ActivityType.Other,  # Sledding
    72: ActivityType.Other,  # Sleeping
    73: ActivityType.Snowboarding,  # Snowboarding
    74: ActivityType.Other,  # Snowmobile
    75: ActivityType.Other,  # Snowshoeing
    76: ActivityType.Other,  # Squash
    77: ActivityType.Other,  # Stair climbing
    78: ActivityType.Other,  # Stair-climbing machine
    79: ActivityType.Other,  # Stand-up paddleboarding
    80: ActivityType.Gym,  # Strength training
    81: ActivityType.Other,  # Surfing
    82: ActivityType.Swimming,  # Swimming
    83: ActivityType.Swimming,  # Swimming (swimming pool)
    84: ActivityType.Swimming,  # Swimming (open water)
    85: ActivityType.Other,  # Table tenis (ping pong)
    86: ActivityType.Other,  # Team sports
    87: ActivityType.Other,  # Tennis
    88: ActivityType.Other,  # Treadmill (walking or running)
    89: ActivityType.Other,  # Volleyball
    90: ActivityType.Other,  # Volleyball (beach)
    91: ActivityType.Other,  # Volleyball (indoor)
    92: ActivityType.Other,  # Wakeboarding
    93: ActivityType.Walking,  # Walking (fitness)
    94: ActivityType.Walking,  # Nording walking
    95: ActivityType.Walking,  # Walking (treadmill)
    96: ActivityType.Other,  # Waterpolo
    97: ActivityType.Gym,  # Weightlifting
    98: ActivityType.Wheelchair,  # Wheelchair
    99: ActivityType.Other,  # Windsurfing
    100: ActivityType.Other,  # Yoga
    101: ActivityType.Gym,  # Zumba
    102: ActivityType.Other,  # Diving
    103: ActivityType.Other,  # Ergometer
    104: ActivityType.Skating,  # Ice skating
    105: ActivityType.Skating,  # Indoor skating
    106: ActivityType.Other,  # Curling
    108: ActivityType.Other,  # Other (unclassified fitness activity)
    # 109-112 are sleep - ignore them
}

# interchange.ActivityType to Google Fit integer
# Most have exact mappings
atype_to_googlefit = {
    ActivityType.Running: 8,
    ActivityType.Cycling: 1,
    ActivityType.MountainBiking: 15,
    ActivityType.Walking: 7,
    ActivityType.Hiking: 35,
    ActivityType.DownhillSkiing: 68,
    ActivityType.CrossCountrySkiing: 67,
    ActivityType.Snowboarding: 73,
    ActivityType.Skating: 62,
    ActivityType.Swimming: 82,
    ActivityType.Wheelchair: 98,
    ActivityType.Rowing: 53,
    ActivityType.Elliptical: 25,
    ActivityType.Gym: 80,       # "Strength training" ?
    ActivityType.Climbing: 52,  # Rock climbing
    ActivityType.Other: 4,
}
