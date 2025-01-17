import os
import csv
from tqdm import tqdm
from collections import defaultdict
from datetime import datetime
import heapq
import numpy as np
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw
from rdp import rdp
from similaritymeasures import frechet_dist
from scipy.spatial import distance


def read_and_group_csv_generator(file_path, chunk_size=5000000):
    """
    Reads a large CSV file in chunks in a memory-efficient way, sorts by MMSI and BaseDateTime,
    and yields rows grouped by MMSI one at a time.

    Args:
        file_path (str): Path to the CSV file.
        chunk_size (int): Number of rows to read per chunk.

    Yields:
        tuple: (MMSI, track), where track is a dictionary containing grouped data.
    """
    temp_files = []

    # Read CSV file in chunks
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        chunk = []
        for row in reader:
            try:
                row['BaseDateTime'] = datetime.strptime(row['BaseDateTime'], '%Y-%m-%dT%H:%M:%S')
            except ValueError as e:
                print(f"Error parsing date: {e}, row: {row}")
                continue

            chunk.append(row)

            # When chunk size is reached, sort and write to temp file
            if len(chunk) >= chunk_size:
                chunk.sort(key=lambda x: (x['MMSI'], x['BaseDateTime']))
                temp_file = f'temp_{len(temp_files)}.csv'
                with open(temp_file, 'w', newline='') as temp_csv:
                    writer = csv.DictWriter(temp_csv, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(chunk)
                temp_files.append(temp_file)
                chunk = []

        # Process the remaining rows
        if chunk:
            chunk.sort(key=lambda x: (x['MMSI'], x['BaseDateTime']))
            temp_file = f'temp_{len(temp_files)}.csv'
            with open(temp_file, 'w', newline='') as temp_csv:
                writer = csv.DictWriter(temp_csv, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(chunk)
            temp_files.append(temp_file)

    # Merge sorted temp files
    temp_readers = [csv.DictReader(open(f, 'r')) for f in temp_files]
    merged_data = heapq.merge(*temp_readers, key=lambda x: (x['MMSI'], datetime.strptime(x['BaseDateTime'], '%Y-%m-%d %H:%M:%S')))

    # Group by MMSI and yield
    current_mmsi = None
    current_track = defaultdict(list)
    for row in merged_data:
        try:
            mmsi = int(row['MMSI'])
        except ValueError:
            print(f"Skipping row due to invalid MMSI: {row['MMSI']}")
            continue

        if mmsi != current_mmsi:
            if current_mmsi is not None:
                yield current_mmsi, current_track  # Yield the current track
            current_mmsi = mmsi
            current_track = defaultdict(list)

        # Append row data to the current track
        current_track['MMSI'].append(mmsi)
        current_track['BaseDateTime'].append(datetime.strptime(row['BaseDateTime'], "%Y-%m-%d %H:%M:%S").timestamp())  # Convert to timestamp
        current_track['LAT'].append(float(row['LAT']))
        current_track['LON'].append(float(row['LON']))
        current_track['SOG'].append(float(row['SOG']))
        current_track['COG'].append(float(row['COG']))
        current_track['Heading'].append(float(row['Heading']))
        current_track['VesselName'].append(row['VesselName'])
        current_track['IMO'].append(row['IMO'])
        current_track['CallSign'].append(row['CallSign'])
        current_track['VesselType'].append(row['VesselType'])
        current_track['Status'].append(row['Status'])
        current_track['Length'].append(row['Length'])
        current_track['Width'].append(row['Width'])
        current_track['Draft'].append(row['Draft'])
        current_track['Cargo'].append(row['Cargo'])
        current_track['TransceiverClass'].append(row['TransceiverClass'])

    # Yield the last track
    if current_mmsi is not None:
        yield current_mmsi, current_track

    # Cleanup temp files
    for temp_file in temp_files:
        os.remove(temp_file)


def _calculate_area(p1, p2, p3):
    """Calculate triangle area using coordinates"""
    return abs(
        (p2[0] - p1[0]) * (p3[1] - p1[1]) -
        (p3[0] - p1[0]) * (p2[1] - p1[1])
    ) / 2


def visvalingam_whyatt(points, threshold):
        """
        Visvalingam-Whyatt simplification for all trajectories
        Args:
            points (numpy.array): Latitude and longitude of AIS data point
            threshold (float): Area threshold for point removal
        Returns:
            list[dict]: Simplified trajectories
        """
        # def vw_method(points, threshold):
        if len(points) < 3:
            return np.ones(len(points), dtype=bool)  # Keep all points
        
        areas = np.zeros(len(points))
        for i in range(1, len(points) - 1):
            areas[i] = _calculate_area(points[i - 1], points[i], points[i + 1])
        
        mask = np.ones(len(points), dtype=bool)
        while True:
            min_area_idx = areas[1:-1].argmin() + 1
            if areas[min_area_idx] > threshold:
                break
            mask[min_area_idx] = False
            # Update neighboring areas
            if min_area_idx > 1:
                areas[min_area_idx - 1] = _calculate_area(
                    points[min_area_idx - 2], points[min_area_idx - 1], points[min_area_idx + 1]
                ) if mask[min_area_idx - 1] else float('inf')
            if min_area_idx < len(points) - 2:
                areas[min_area_idx + 1] = _calculate_area(
                    points[min_area_idx - 1], points[min_area_idx + 1], points[min_area_idx + 2]
                ) if mask[min_area_idx + 1] else float('inf')
            areas[min_area_idx] = float('inf')
        
        return mask


def douglas_peucker(points, epsilon):
    """
    Douglas-Peucker simplification for all trajectories.

    Args:
        points (numpy.array): Latitude and longitude of AIS data points.
        epsilon (float): Distance threshold for point removal.

    Returns:
        numpy.array: Simplified points.
    """
    mask = rdp(points, epsilon=epsilon, return_mask=True)
    return mask

# Time-dependent Trajectory Reduction (TD-TR) Algorithm

def td_tr(points, time, threshold):
    """
    Time-Dependent Trajectory Reduction (TD-TR) for all trajectories.

    Args:
        points (numpy.array): Latitude and longitude of AIS data points.
        time (numpy.array): Corresponding time points.
        threshold (float): Distance threshold for point removal.

    Returns:
        numpy.array: Mask indicating points to keep.
    """
    if len(points) < 3:
        return np.ones(len(points), dtype=bool)  # Keep all points

    mask = np.ones(len(points), dtype=bool)
    for i in range(1, len(points) - 1):
        t_s, x_s, y_s = time[i - 1], points[i - 1][0], points[i - 1][1]
        t_e, x_e, y_e = time[i + 1], points[i + 1][0], points[i + 1][1]
        t_i, x_i, y_i = time[i], points[i][0], points[i][1]

        # Calculate synchronized position (x', y') at time t_i
        x_prime = x_s + (x_e - x_s) * (t_i - t_s) / (t_e - t_s)
        y_prime = y_s + (y_e - y_s) * (t_i - t_s) / (t_e - t_s)

        # Calculate the Euclidean distance between (x_i, y_i) and (x_prime, y_prime)
        dist = np.sqrt((x_i - x_prime) ** 2 + (y_i - y_prime) ** 2)
        if dist < threshold:
            mask[i] = False

    return mask


def eval_simplification(track_origin, track_simple):
    """
    Evaluating trajectory simplification results by Simplification Rate (SR), Length Loss Rate (LLS),
    and evaluating the similarity between simplified and the original trajectories by 
    Dynamic Time Warping (DTW), Frechet distance, and Average Synchronized Euclidean Distance (ASED).
    
    Args:
        track_origin (dict): Original trajectory with keys 'LAT', 'LON', and 'BaseDateTime'.
        track_simple (dict): Simplified trajectory with keys 'LAT', 'LON', and 'BaseDateTime'.
        
    Returns:
        dict: {'SR': float, 'LLR': float, 'DTW': float, 'Frechet': float, 'ASED': float}
    """
    # Convert tracks to numpy arrays for easier manipulation
    origin_points = np.column_stack((track_origin['BaseDateTime'], track_origin['LON'], track_origin['LAT']))
    simple_points = np.column_stack((track_simple['BaseDateTime'], track_simple['LON'], track_simple['LAT']))

    # Simplification Rate (SR)
    SR = (len(origin_points) - len(simple_points)) / len(origin_points)

    # Length Loss Rate (LLR)
    def calculate_length(points):
        return np.sum(np.linalg.norm(np.diff(points[:, 1:], axis=0), axis=1))
    
    length_origin = calculate_length(origin_points)
    length_simple = calculate_length(simple_points)
    LLR = (length_origin - length_simple) / length_origin   # smaller is better

    # Dynamic Time Warping (DTW)
    DTW, _ = fastdtw(origin_points[:, 1:], simple_points[:, 1:], dist=euclidean)

    # Frechet Distance
    Frechet = frechet_dist(origin_points[:, 1:], simple_points[:, 1:])
    
    # Average Synchronized Euclidean Distance (ASED)
    def calculate_ased(origin_points, simple_points):
        ASED = 0
        for i in range(1, len(origin_points) - 1):
            t_s, x_s, y_s = origin_points[i - 1]
            t_e, x_e, y_e = origin_points[i + 1]
            t_i, x_i, y_i = origin_points[i]
            
            # Calculate synchronized position (x', y') at time t_i
            t_s = float(t_s)
            t_e = float(t_e)
            t_i = float(t_i)
            x_prime = x_s + (x_e - x_s) * (t_i - t_s) / (t_e - t_s)
            y_prime = y_s + (y_e - y_s) * (t_i - t_s) / (t_e - t_s)
            
            # Calculate the Euclidean distance between (x_i, y_i) and (x_prime, y_prime)
            ASED += np.sqrt((x_i - x_prime) ** 2 + (y_i - y_prime) ** 2)
        
        return ASED / len(origin_points)
    
    ASED = calculate_ased(origin_points, simple_points)

    # Basic information of the original track
    mmsi = track_origin['MMSI'][0]
    num_points = len(track_origin['BaseDateTime'])
    vessel_type = track_origin['VesselType'][0]

    return {
        'SR': SR,
        'LLR': LLR,
        'DTW': DTW,
        'Frechet': Frechet,
        'ASED': ASED,
        'mmsi': mmsi,
        'length_origin': length_origin,
        'point_origin': num_points,
        'ship_type': vessel_type,
    }
    # return {
    #     'SR': SR,
    #     'LLR': LLR,
    #     'DTW': DTW,
    #     'Frechet': Frechet,
    #     'ASED': ASED,
    # }


def write_and_save_dict(compressed_track, save_path, first_write):
    """
    Write a single dictionary (compressed track) to a CSV file.

    Args:
        compressed_track (dict): The compressed trajectory data.
        save_path (str): The output CSV file path.
        first_write (bool): Whether this is the first write (for header inclusion).
    """
    mode = 'a' if not first_write else 'w'  # Append after the first write
    with open(save_path, mode=mode, newline='') as file:
        writer = csv.DictWriter(file, fieldnames=compressed_track.keys())
        if first_write:
            writer.writeheader()  # Write header only for the first write

        # Write rows
        rows = zip(*[compressed_track[key] for key in compressed_track.keys()])
        for row in rows:
            writer.writerow(dict(zip(compressed_track.keys(), row)))


def write_and_save_eval_metrics(metric_dict, save_path, first_write):
    """
    Write a single dictionary (compressed track) to a CSV file.

    Args:
        metric_dict (dict): The evaluation results of simplified trajectory (monthly, single)
        save_path (str): The output CSV file path.
        first_write (bool): Whether this is the first write (for header inclusion).
    """
    mode = 'a' if not first_write else 'w'  # Append after the first write
    with open(save_path, mode=mode, newline='') as file:
        writer = csv.DictWriter(file, fieldnames=metric_dict.keys())
        if first_write:
            writer.writeheader()  # Write header only for the first write
        writer.writerow(metric_dict)


if __name__ == "__main__":
    input_folder = './merged/'
    # input_folder = './zip/'
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    # output_folder = './compressed_vw/'
    # output_folder = './compressed_rdp/'
    # output_folder = './compressed_tdtr/'
    output_folder = './compressed/month/'
    simp_algorithm = 'vw' # one of 'vw', 'rdp', 'tdtr'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        write_path = os.path.join(output_folder, f'{simp_algorithm}_{file}')
        eval_path = os.path.join(output_folder, f'eval_{simp_algorithm}_{file}')
        # grouped_data = read_and_group_csv(file_path)
        # Print grouped data for verification
        # simplified_trajectories = []
        first_write = True
        # for mmsi, track in tqdm(grouped_data.items(), desc="Compressing tracks"):
        for mmsi, track in tqdm(read_and_group_csv_generator(file_path), desc="Compressing tracks"):
            first = False
            points = np.column_stack((track['LON'], track['LAT']))
            mask = visvalingam_whyatt(points, threshold=0.000001)
            # mask = douglas_peucker(points, epsilon=0.1)
            # mask = td_tr(points, track['BaseDateTime'], 0.1)
            compressed_track = dict(
                **{k: np.array(track[k])[mask] for k in track.keys()},
            )
            # print(f"MMSI: {mmsi}, track length: {len(track['LON'])}, compressed length: {len(compressed_track['LON'])}")
            # simplified_trajectories.append(compressed_track)
            write_and_save_dict(compressed_track, write_path, first_write)
            write_and_save_eval_metrics(eval_simplification(track, compressed_track), eval_path, first_write)
            first_write = False

        break