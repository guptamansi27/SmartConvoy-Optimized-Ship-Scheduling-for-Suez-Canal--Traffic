from flask import Flask, request, render_template_string
import pandas as pd
import os
from pyngrok import ngrok

excel_file = None  
public_url = None

def start_tunnel():
    global public_url
    public_url = str(ngrok.connect(5000))
    print("Public URL:", public_url)
    return public_url

def stop_tunnel():
    ngrok.kill()


app = Flask(__name__)


html_form = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Vessel Information Form</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
            color: #333;
        }

        .form-card {
            background: #ffffff;
            width: 100%;
            max-width: 520px;
            border-radius: 20px;
            padding: 30px 28px;
            box-shadow: 0 20px 50px rgba(0,0,0,0.25);
            animation: slideUp 0.8s ease;
        }

        @keyframes slideUp {
            from { transform: translateY(30px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }

        .header {
            text-align: center;
            margin-bottom: 20px;
        }

        .header h1 {
            color: #1f3c88;
            font-size: 1.6rem;
            margin-bottom: 5px;
        }

        .header p {
            font-size: 0.95rem;
            color: #777;
        }

        .form-group {
            margin-bottom: 14px;
        }

        label {
            font-size: 0.85rem;
            font-weight: 600;
            color: #444;
            display: block;
            margin-bottom: 4px;
        }

        input, select {
            width: 100%;
            padding: 11px 12px;
            border: 1px solid #ddd;
            border-radius: 10px;
            font-size: 0.95rem;
            transition: 0.2s ease;
        }

        input:focus, select:focus {
            border-color: #1f3c88;
            box-shadow: 0 0 0 3px rgba(31,60,136,0.1);
            outline: none;
        }

        .btn-submit {
            margin-top: 15px;
            width: 100%;
            padding: 13px;
            background: linear-gradient(135deg, #1f3c88, #3a7bd5);
            border: none;
            color: white;
            font-size: 1.05rem;
            font-weight: 600;
            border-radius: 12px;
            cursor: pointer;
            transition: 0.3s ease;
        }

        .btn-submit:hover {
            transform: translateY(-1px);
            box-shadow: 0 10px 25px rgba(31,60,136,0.3);
        }

        .success {
            text-align: center;
            font-size: 1.1rem;
            color: #1f8b4c;
        }

        .required {
            color: #e74c3c;
        }
    </style>
</head>

<body>

<div class="form-card">
    <div class="header">
        <h1>Vessel Data Form</h1>
        <p>Enter ship details accurately</p>
    </div>

    <form method="POST">

        <div class="form-group">
            <label>Length (m)</label>
            <input type="number" step="any" name="length">
        </div>

        <div class="form-group">
            <label>Beam (Max Width) (m)</label>
            <input type="number" step="any" name="beam">
        </div>

        <div class="form-group">
            <label>Draft (Depth) (m)</label>
            <input type="number" step="any" name="draft">
        </div>

        <div class="form-group">
            <label>SCGT (Volume) (tons)</label>
            <input type="number" step="any" name="scgt">
        </div>

        <div class="form-group">
            <label>Speed (knots)</label>
            <input type="number" step="any" name="speed">
        </div>

        <div class="form-group">
            <label>Cargo Type</label>
            <select name="cargo_type">
                <option value="">Select cargo type</option>
                <option>Container</option>
                <option>Vehicle</option>
                <option>Bulk Carrier</option>
                <option>Product Tanker</option>
                <option>Reefer</option>
                <option>LNG</option>
                <option>Chemical</option>
                <option>General Cargo</option>
                <option>LPG</option>
                <option>Crude Oil</option>

            </select>
        </div>

        <div class="form-group">
            <label>Direction</label>
            <select name="direction">
                <option value="">Select direction</option>
                <option>NorthBound</option>
                <option>SouthBound</option>
            </select>
        </div>

        <div class="form-group">
            <label>Arrival Date</label>
            <input type="date" name="arrival_date">
        </div>

        <div class="form-group">
            <label>Email <span class="required">*</span></label>
            <input type="email" name="email" required placeholder="example@email.com">
        </div>

        <div class="form-group">
            <label>Phone No.</label>
            <input type="tel" name="phone" pattern="[0-9]{10}" placeholder="10-digit number">
        </div>

        <button type="submit" class="btn-submit">Submit Details</button>

    </form>
</div>

</body>
</html>
"""


@app.route("/", methods=["GET", "POST"])
def form():
    global excel_file

    if excel_file is None:
        return "<h3>No Excel file path found. Please select folder in GUI.</h3>"

    if request.method == "POST":
        data = {
            "Length (m)": request.form.get("length"),
            "Beam(Max Width) (m)": request.form.get("beam"),
            "Draft(depth) (m)": request.form.get("draft"),
            "SCGT(Volume) (tons)": request.form.get("scgt"),
            "Speed (knots)": request.form.get("speed"),
            "Cargo Type": request.form.get("cargo_type"),
            "Direction": request.form.get("direction"),
            "Arrival Date": request.form.get("arrival_date"),
            "Email": request.form.get("email"),
            "Phone No.": request.form.get("phone"),
        }

        if os.path.exists(excel_file):
            df = pd.read_excel(excel_file)
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        else:
            df = pd.DataFrame([data])

        df.to_excel(excel_file, index=False)
        return "<h3>Data Saved Successfully!</h3>"

    return render_template_string(html_form)


def start_flask():
    start_tunnel()
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
