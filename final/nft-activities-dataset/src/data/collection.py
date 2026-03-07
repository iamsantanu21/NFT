class Collection:
    def __init__(self):
        self.activities = []

    def add_activity(self, activity):
        self.activities.append(activity)

    def get_activities_by_type(self, activity_type):
        return [activity for activity in self.activities if activity['type'] == activity_type]

    def filter_activities_by_timestamp(self, start_timestamp, end_timestamp):
        return [
            activity for activity in self.activities
            if start_timestamp <= activity['timestamp'] <= end_timestamp
        ]