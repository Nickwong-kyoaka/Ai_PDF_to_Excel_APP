import base64
import json
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pdf2image import convert_from_path
import pandas as pd
import requests
from PIL import Image
import io
import threading  # For non-blocking processing
from openai import AzureOpenAI  # Import for Azure

# Hardcoded Poppler path
POPPLER_PATH = r"C:/Program Files/poppler-25.12.0/Library/bin"

def image_to_base64(image):
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode('utf-8')

def analyze_page_with_api(image_base64, page_num, api_key, base_url, model, provider, deployment_name=None, api_version=None, max_tokens=2048, temperature=0.1):
    prompt = """
You are an expert at reading scanned handwritten questionnaires.
Analyze this page (page {page_num}) carefully.

Tasks:
1. Detect the Participant ID: PRIORITIZE handwritten 'Axxx' (e.g., 'A001', 'A002', 'A004') in the top corner if present. If not, use the 'Participant ID' field (numerical like '136513240329'). If none, use 'Unknown'.
2. Extract all headers, questions, options, and selected answers (detect circles, ticks, crosses, or handwritten responses).
3. For scales (e.g., 0-5 tables), detect circled numbers per row/question.
4. Include bilingual text (English and Tagalog) if present.
5. If a new questionnaire starts (e.g., consent form repeat), note it.

Output **only** valid JSON, no extra text:
{{
  "participant_id": "detected ID like A001",
  "elements": [
    {{
      "element_type": "Header" or "Question" or "Section" or "Table",
      "page_number": {page_num},
      "question_number": "1" or "N/A",
      "question_text": "full question or header text (include English and Tagalog)",
      "options": "comma-separated options or N/A",
      "selected_answer": "detected answer (e.g., 'Yes' circled, '3' circled, handwritten text)",
      "notes": "any extra text, confidence, or unclear parts"
    }},
    ...
  ]
}}
Be extremely precise with handwriting and visual marks (circles around numbers, ticks in boxes).
""".format(page_num=page_num).strip()

    messages = [
        {"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_base64}"}}
        ]}
    ]

    if provider == "Azure OpenAI":
        # Use AzureOpenAI client
        client = AzureOpenAI(
            api_version=api_version,
            azure_endpoint=base_url,
            api_key=api_key,
        )
        response = client.chat.completions.create(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            model=deployment_name  # Deployment name for Azure
        )
        content = response.choices[0].message.content
    else:
        # Use requests for xAI/OpenAI
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        url = f"{base_url.rstrip('/')}/chat/completions"
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]

    content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    
    try:
        data = json.loads(content)
        return data["participant_id"], data["elements"]
    except Exception as e:
        print(f"JSON parse failed on page {page_num}: {e}")
        print("Raw content:", content)
        return "Unknown", []

def process_pdf(pdf_path, output_folder, api_key, base_url, model, provider, deployment_name, api_version, max_tokens, temperature, filename_prefix, combine_flag, progress_var, status_label, root):
    images = convert_from_path(pdf_path, poppler_path=POPPLER_PATH)
    total_pages = len(images)
    progress_var.set(0)
    
    data_by_id = {}
    current_id = None  # Start with None
    id_counter = 1

    for page_num, image in enumerate(images, start=1):
        status_label.config(text=f"Processing page {page_num}/{total_pages}...")
        root.update_idletasks()  # Update GUI
        
        image_base64 = image_to_base64(image)
        page_id, elements = analyze_page_with_api(image_base64, page_num, api_key, base_url, model, provider, deployment_name, api_version, max_tokens, temperature)
        
        # Grouping: Only switch if new non-Unknown ID detected and different
        if page_id != "Unknown":
            if current_id is None or page_id != current_id:
                current_id = page_id
        
        # If still None (first page Unknown), start Unknown_1
        if current_id is None:
            current_id = f"Unknown_{id_counter}"
            id_counter += 1
        
        if current_id not in data_by_id:
            data_by_id[current_id] = []
        for elem in elements:
            elem["participant_id"] = current_id
        data_by_id[current_id].extend(elements)
        
        progress_var.set((page_num / total_pages) * 100)
        root.update_idletasks()

    # Save to a single Excel file
    excel_filename = f"{filename_prefix}.xlsx" if filename_prefix else "Questionnaires.xlsx"
    excel_path = os.path.join(output_folder, excel_filename)
    
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        if combine_flag:
            # Combine all into one sheet
            all_columns = []
            header = []
            keys = ['element_type', 'page_number', 'question_number', 'question_text', 'selected_answer', 'notes']  # Dropped options and participant_id
            for pid, elements in data_by_id.items():
                if elements:
                    df = pd.DataFrame(elements)
                    df = df[keys]
                    num_elements = len(elements)
                    header.extend([pid] * num_elements)
                    all_columns.extend(df.values.T.tolist())
            if all_columns:
                combined_df = pd.DataFrame(all_columns, index=keys, columns=range(len(header)))
                combined_df.loc['participant_id'] = header
                combined_df = combined_df.reindex(['participant_id'] + keys)
                combined_df.to_excel(writer, index=True, header=False, sheet_name="Combined")
                print("Added combined sheet")
        else:
            # Original: one sheet per ID
            for pid, elements in data_by_id.items():
                if elements:
                    df = pd.DataFrame(elements)
                    df = df.drop(columns=['options', 'participant_id'])
                    df = df.T
                    df.to_excel(writer, index=True, header=False, sheet_name=pid[:31])
                    print(f"Added transposed sheet for {pid}")
    
    status_label.config(text="Finished! ✅")
    messagebox.showinfo("Success", f"Excel file saved: {excel_path}")

# GUI App
def main_app():
    root = tk.Tk()
    root.title("Questionnaire Extractor App")
    root.geometry("600x600")
    root.resizable(True, True)

    # Frame for AI settings
    ai_frame = ttk.LabelFrame(root, text="AI Settings")
    ai_frame.pack(pady=10, padx=10, fill="x")

    # AI Provider
    tk.Label(ai_frame, text="AI Provider:").grid(row=0, column=0, pady=5, sticky="w")
    provider_var = tk.StringVar(value="xAI")
    provider_dropdown = ttk.Combobox(ai_frame, textvariable=provider_var, values=["xAI", "OpenAI", "Azure OpenAI"])
    provider_dropdown.grid(row=0, column=1, pady=5, sticky="ew")

    # API Key
    tk.Label(ai_frame, text="API Key:").grid(row=1, column=0, pady=5, sticky="w")
    api_key_entry = tk.Entry(ai_frame, width=50)
    api_key_entry.grid(row=1, column=1, pady=5, sticky="ew")

    # Base URL / Endpoint
    tk.Label(ai_frame, text="Base URL / Endpoint:").grid(row=2, column=0, pady=5, sticky="w")
    base_url_entry = tk.Entry(ai_frame, width=50)
    base_url_entry.insert(0, "https://api.x.ai/v1")
    base_url_entry.grid(row=2, column=1, pady=5, sticky="ew")

    # Model Name
    tk.Label(ai_frame, text="Model Name:").grid(row=3, column=0, pady=5, sticky="w")
    model_entry = tk.Entry(ai_frame, width=50)
    model_entry.insert(0, "grok-4")
    model_entry.grid(row=3, column=1, pady=5, sticky="ew")

    # Deployment Name (for Azure)
    deployment_label = tk.Label(ai_frame, text="Deployment Name:")
    deployment_label.grid(row=4, column=0, pady=5, sticky="w")
    deployment_entry = tk.Entry(ai_frame, width=50)
    deployment_entry.grid(row=4, column=1, pady=5, sticky="ew")
    deployment_entry.grid_remove()  # Hide initially
    deployment_label.grid_remove()

    # API Version (for Azure)
    api_version_label = tk.Label(ai_frame, text="API Version:")
    api_version_label.grid(row=5, column=0, pady=5, sticky="w")
    api_version_entry = tk.Entry(ai_frame, width=50)
    api_version_entry.grid(row=5, column=1, pady=5, sticky="ew")
    api_version_entry.grid_remove()  # Hide initially
    api_version_label.grid_remove()

    # Max Tokens
    tk.Label(ai_frame, text="Max Tokens:").grid(row=6, column=0, pady=5, sticky="w")
    max_tokens_entry = tk.Entry(ai_frame, width=50)
    max_tokens_entry.insert(0, "2048")  # Default
    max_tokens_entry.grid(row=6, column=1, pady=5, sticky="ew")

    # Temperature
    tk.Label(ai_frame, text="Temperature:").grid(row=7, column=0, pady=5, sticky="w")
    temperature_entry = tk.Entry(ai_frame, width=50)
    temperature_entry.insert(0, "0.1")  # Default
    temperature_entry.grid(row=7, column=1, pady=5, sticky="ew")

    # Update defaults and show/hide fields
    def update_defaults(*args):
        provider = provider_var.get()
        if provider == "xAI":
            base_url_entry.delete(0, tk.END)
            base_url_entry.insert(0, "https://api.x.ai/v1")
            model_entry.delete(0, tk.END)
            model_entry.insert(0, "grok-4")
            deployment_entry.grid_remove()
            deployment_label.grid_remove()
            api_version_entry.grid_remove()
            api_version_label.grid_remove()
        elif provider == "OpenAI":
            base_url_entry.delete(0, tk.END)
            base_url_entry.insert(0, "https://api.openai.com/v1")
            model_entry.delete(0, tk.END)
            model_entry.insert(0, "gpt-4o")
            deployment_entry.grid_remove()
            deployment_label.grid_remove()
            api_version_entry.grid_remove()
            api_version_label.grid_remove()
        elif provider == "Azure OpenAI":
            base_url_entry.delete(0, tk.END)
            base_url_entry.insert(0, "https://openai.openai.azure.com/")
            model_entry.delete(0, tk.END)
            model_entry.insert(0, "gpt-4o")
            deployment_entry.delete(0, tk.END)
            deployment_entry.insert(0, "GPT-4o")
            deployment_entry.grid()
            deployment_label.grid()
            api_version_entry.delete(0, tk.END)
            api_version_entry.insert(0, "2024-12-01-preview")
            api_version_entry.grid()
            api_version_label.grid()

    provider_dropdown.bind("<<ComboboxSelected>>", update_defaults)

    # Frame for File settings
    file_frame = ttk.LabelFrame(root, text="File Settings")
    file_frame.pack(pady=10, padx=10, fill="x")

    # PDF Path
    pdf_path_var = tk.StringVar()
    tk.Label(file_frame, text="PDF File:").grid(row=0, column=0, pady=5, sticky="w")
    tk.Entry(file_frame, textvariable=pdf_path_var, width=50).grid(row=0, column=1, pady=5, sticky="ew")
    def select_pdf():
        path = filedialog.askopenfilename(filetypes=[("PDF files", "*.pdf")])
        pdf_path_var.set(path)
    tk.Button(file_frame, text="Browse", command=select_pdf).grid(row=0, column=2, pady=5)

    # Output Folder
    output_folder_var = tk.StringVar()
    tk.Label(file_frame, text="Output Folder:").grid(row=1, column=0, pady=5, sticky="w")
    tk.Entry(file_frame, textvariable=output_folder_var, width=50).grid(row=1, column=1, pady=5, sticky="ew")
    def select_folder():
        path = filedialog.askdirectory()
        output_folder_var.set(path)
    tk.Button(file_frame, text="Browse", command=select_folder).grid(row=1, column=2, pady=5)

    # Filename Prefix
    tk.Label(file_frame, text="Excel Filename Prefix:").grid(row=2, column=0, pady=5, sticky="w")
    filename_prefix_entry = tk.Entry(file_frame, width=50)
    filename_prefix_entry.insert(0, "Questionnaire")  # Default
    filename_prefix_entry.grid(row=2, column=1, pady=5, sticky="ew")

    # Combine checkbox
    combine_var = tk.BooleanVar(value=False)
    tk.Checkbutton(file_frame, text="Combine all participants in one sheet", variable=combine_var).grid(row=3, column=0, columnspan=3, pady=5, sticky="w")

    # Progress Bar and Status
    progress_var = tk.DoubleVar()
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=400, mode="determinate", variable=progress_var)
    progress_bar.pack(pady=10)

    status_label = tk.Label(root, text="Ready")
    status_label.pack(pady=5)

    # Process Button
    def start_process():
        pdf_path = pdf_path_var.get()
        output_folder = output_folder_var.get()
        api_key = api_key_entry.get()
        base_url = base_url_entry.get()
        model = model_entry.get()
        provider = provider_var.get()
        deployment_name = deployment_entry.get() if provider == "Azure OpenAI" else None
        api_version = api_version_entry.get() if provider == "Azure OpenAI" else None
        combine_flag = combine_var.get()
        try:
            max_tokens = int(max_tokens_entry.get())
            temperature = float(temperature_entry.get())
        except ValueError:
            messagebox.showerror("Error", "Max Tokens and Temperature must be numbers!")
            return
        
        if not all([pdf_path, output_folder, api_key, base_url, model]):
            messagebox.showerror("Error", "All required fields must be filled!")
            return
        if provider == "Azure OpenAI" and not all([deployment_name, api_version]):
            messagebox.showerror("Error", "Deployment Name and API Version required for Azure!")
            return
        
        # Run in thread to avoid freezing GUI
        threading.Thread(target=process_pdf, args=(pdf_path, output_folder, api_key, base_url, model, provider, deployment_name, api_version, max_tokens, temperature, filename_prefix_entry.get(), combine_flag, progress_var, status_label, root)).start()

    tk.Button(root, text="Start Processing", command=start_process).pack(pady=20)

    root.mainloop()

if __name__ == "__main__":
    main_app()