import itertools
from collections import defaultdict
from tapiriik.services.interchange import WaypointType

def pairwise(gen):
    x, y = itertools.tee(gen)
    next(y, None)
    return zip(x, y)


class AutoPauseCalculator:
    @classmethod
    def calculate(cls, waypoints, target_duration):
        if not waypoints:
            yield from ()

        if type(target_duration) not in [float, int]:
            target_duration = target_duration.total_seconds()

        # First, get a list of the inter-waypoint durations and distance deltas
        inter_wp_times = []
        inter_wp_distances_with_times = [] # Not in any real units
        delta_t_frequencies = defaultdict(int)
        for wp_a, wp_b in pairwise(waypoints):
            delta_t = (wp_b.Timestamp - wp_a.Timestamp).total_seconds()
            delta_t_frequencies[round(delta_t)] += 1
            inter_wp_times.append(delta_t)
            if wp_a.Location and wp_b.Location and wp_a.Location.Latitude is not None and wp_b.Location.Latitude is not None:
                inter_wp_distances_with_times.append(((wp_a.Location.Latitude - wp_b.Location.Latitude) ** 2 + (wp_a.Location.Longitude - wp_b.Location.Longitude) ** 2, delta_t))

        inter_wp_times.sort(reverse=True)
        inter_wp_distances_with_times.sort(key=lambda x: x[0])

        # Guesstimate what the sampling rate is
        delta_t_mode = sorted(delta_t_frequencies.items(), key=lambda x: x[1])[-1][0]

        # ...should sum to the elapsed duration, so we'll cheat
        elapsed_duration = (waypoints[-1].Timestamp - waypoints[0].Timestamp).total_seconds()

        # Then, walk through our list until we recover enough time - call this the auto-pause threshold for time
        # This is an attempt to discover times when they paused the activity (missing data for a significant period of time)
        recovered_duration = 0

        auto_pause_time_threshold = None
        inter_times_iter = iter(inter_wp_times)
        try:
            while elapsed_duration - recovered_duration > target_duration:
                new_thresh = next(inter_times_iter)
                # Bail out before we enter the zone of pausing the entire activity
                if new_thresh <= delta_t_mode * 2:
                    break
                auto_pause_time_threshold = new_thresh
                recovered_duration += auto_pause_time_threshold
        except StopIteration:
            pass

        # And the same for distances, if we didn't find enough time via the inter-waypoint time method
        # This is the traditional "auto-pause" where, if the user is stationary the activity is paused
        # So, we look for points where they were moving the least and pause during them
        auto_pause_dist_threshold = None
        inter_dist_iter = iter(inter_wp_distances_with_times)
        try:
            while elapsed_duration - recovered_duration > target_duration:
                auto_pause_dist_threshold, delta_t = next(inter_dist_iter)
                recovered_duration += delta_t
        except StopIteration:
            pass

        if auto_pause_dist_threshold == 0:
            raise ValueError("Bad auto-pause distance threshold %f" % auto_pause_dist_threshold)

        # Then re-iter through our waypoints and return wapoint type (regular/pause/resume) for each
        # We do this instead of overwriting the waypoint values since that would mess up uploads to other serivces that don't want this automatic calculation
        # We decrement recovered_duration back to 0 and stop adding pauses after that point, in the hopes of having the best success hitting the target duration
        in_pause = False
        for wp_a, wp_b in pairwise(waypoints):
            delta_t = (wp_b.Timestamp - wp_a.Timestamp).total_seconds()
            delta_d = None
            if wp_a.Location and wp_b.Location and wp_a.Location.Latitude is not None and wp_b.Location.Latitude is not None:
                delta_d = (wp_a.Location.Latitude - wp_b.Location.Latitude) ** 2 + (wp_a.Location.Longitude - wp_b.Location.Longitude) ** 2
            if ((auto_pause_time_threshold is not None and delta_t > auto_pause_time_threshold) or (auto_pause_dist_threshold is not None and delta_d is not None and delta_d < auto_pause_dist_threshold)) and recovered_duration > 0:
                recovered_duration -= delta_t
                yield WaypointType.Pause
                in_pause = True
            else:
                yield WaypointType.Resume if in_pause else WaypointType.Regular
                in_pause = False

        # Since we were iterating pairwise above, we need 1 extra for the last waypoint
        yield WaypointType.Resume if in_pause else WaypointType.Regular
