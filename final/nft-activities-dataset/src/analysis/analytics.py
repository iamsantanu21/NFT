def calculate_total_sales(activities):
    total_sales = sum(activity['price'] for activity in activities if activity['type'] == 'sale')
    return total_sales

def calculate_average_price(activities):
    sales = [activity['price'] for activity in activities if activity['type'] == 'sale']
    average_price = sum(sales) / len(sales) if sales else 0
    return average_price

def generate_activity_report(activities):
    report = {}
    for activity in activities:
        activity_type = activity['type']
        if activity_type not in report:
            report[activity_type] = 0
        report[activity_type] += 1
    return report