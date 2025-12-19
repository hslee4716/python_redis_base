from redis_load_balancer import WEIGHTS_DIR, DEVICE
import os

from annotated_types import IsDigit
from paddleocr import PaddleOCR
import numpy as np
import paddle

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


default_detection_model_dir = WEIGHTS_DIR / "PP-OCRv5_mobile_det"
default_recognition_model_dir = WEIGHTS_DIR / "korean_PP-OCRv5_mobile_rec"

font_path = WEIGHTS_DIR / "NanumGothic.ttf"
unicode_font = ImageFont.truetype(str(font_path), 10)

def flatten_char_list(word_list, word_col_list):
    word_list_clean = []
    word_col_list_clean = []
    for i in range(len(word_list)):
        temp = []
        [temp.extend(ww) for ww in word_list[i]]
        word_list_clean.append(temp)
        
        temp = []
        [temp.extend(cc) for cc in word_col_list[i]]
        word_col_list_clean.append(temp)
    return word_list_clean, word_col_list_clean

def draw_text(img, text, bbox, fontsz=None, fill=(0, 255, 0), center=True, x_shift=0, y_shift=0):
    flag = False
    x1, y1, x2, y2 = bbox
    if fontsz is None:
        fontsz = int((y2 - y1) * 0.6)
    if isinstance(img, np.ndarray):
        img = Image.fromarray(img)
        flag = True
    draw = ImageDraw.Draw(img)
    
    # draw text with appropriate font size
    unicode_font = ImageFont.truetype(str(font_path), fontsz)
    
    
    # Get text bounding box to calculate proper centering
    bbox_text = draw.textbbox((0, 0), text, font=unicode_font)
    text_width = bbox_text[2] - bbox_text[0]  # width
    text_height = bbox_text[3] - bbox_text[1]  # height
    
    # Calculate center position within the given bbox
    bbox_width = x2 - x1
    bbox_height = y2 - y1
    
    # Center the text within the bbox
    if center:
        text_x = x1 + (bbox_width - text_width) // 2
        text_y = y1 + (bbox_height - text_height) // 2
    else:
        text_x = x1
        text_y = y1
    
    draw.text((text_x+x_shift, text_y+y_shift), text, fill=fill, font=unicode_font)
    if flag:
        img = np.array(img)
    return img

class OCRProcessor:
    def __init__(self, model_dir=WEIGHTS_DIR, fast=True, det_options={}):
        # det_options={'return_word_box': True}
        self.det_options = det_options
        if model_dir:
            text_detection_model_name="PP-OCRv5_mobile_det"
            text_detection_model_dir=f"{model_dir}/PP-OCRv5_mobile_det"
            text_recognition_model_name="korean_PP-OCRv5_mobile_rec"
            text_recognition_model_dir=f"{model_dir}/korean_PP-OCRv5_mobile_rec"
            
            self.model = PaddleOCR(lang="korean", precision="fp16",
                                   use_textline_orientation=not fast,
                                   use_doc_orientation_classify=not fast,
                                   use_doc_unwarping=not fast,
                                   text_detection_model_name=text_detection_model_name,
                                   text_detection_model_dir=text_detection_model_dir,
                                   text_recognition_model_name=text_recognition_model_name,
                                   text_recognition_model_dir=text_recognition_model_dir)
        else:
            self.model = PaddleOCR(lang="korean", precision="fp16",
                                   use_textline_orientation=not fast,
                                   use_doc_orientation_classify=not fast,
                                   use_doc_unwarping=not fast
                                   )
    
    def ocr(self, im):
        temp = self.det_options.copy()
        temp['text_det_box_thresh'] = 0.6
        # temp['text_det_thresh'] = OCR_TEXT_THRESH
            
        result = self.model.predict(im, **temp)
        res = result[0]
        bboxes = res['rec_boxes']
        scores = res['rec_scores']
        texts = res['rec_texts']
        out = []
        for box, score, text in zip(bboxes, scores, texts):
            if isinstance(box, np.ndarray):
                box = box.tolist()
            if isinstance(score, np.ndarray):
                score = score.tolist()
            out.append({
                'coords': box,
                'score': score,
                'text': text
            })
        return out

    @staticmethod
    def calculate_bbox_char_by_char(full_text, char_list, char_col_list, line_coords, cell_width):
        """
        Calculate bounding box for each character within a text line
        
        Args:
            full_text: The full text of the line
            char_list: List of characters
            char_col_list: List of character cell indices (not pixel offsets)
            line_coords: Original line bounding box [x1, y1, x2, y2]
            cell_width: Width of each character cell in pixels
        
        Returns:
            List of [x1, y1, x2, y2] coordinates for each character
        """
        line_coords = list(line_coords)
        
        # Extract bbox coordinates
        bbox_x_start, bbox_y_start, bbox_x_end, bbox_y_end = line_coords
        
        # Validate input lengths
        if len(char_list) != len(char_col_list):
            return []
        
        result = []
        
        # Calculate bounding box for each character
        for i in range(len(char_list)):
            # Validate index
            if i >= len(char_col_list):
                continue
            
            # Calculate x coordinates using cell indices
            # Similar to calculate_word_bbox but for single character
            char_x_start = bbox_x_start + int(char_col_list[i] * cell_width - cell_width*2)  # padding_left
            char_x_end = bbox_x_start + int((char_col_list[i] + 1) * cell_width + cell_width*2)  # padding_right
            
            # Return as [x1, y1, x2, y2]
            char_bbox = [int(char_x_start), int(bbox_y_start), int(char_x_end), int(bbox_y_end)]
            result.append(char_bbox)
        
        return result


    @staticmethod
    def calculate_word_bbox(word, full_text, char_list, char_col_list, line_coords, cell_width):
        """
        Calculate bounding box for a specific word within a text line
        
        Args:
            word: The word to find bbox for
            full_text: The full text of the line
            char_list: List of characters
            char_col_list: List of character cell indices (not pixel offsets)
            line_coords: Original line bounding box [x1, y1, x2, y2]
            cell_width: Width of each character cell in pixels
        
        Returns:
            [x1, y1, x2, y2] coordinates for the word
        """
        line_coords = list(line_coords)
        
        # Find the word position in full text
        start_idx = full_text.find(word)
        if start_idx == -1:
            # If exact match not found, return original line coords
            return line_coords
        
        end_idx = start_idx + len(word) - 1  # Index of last character
        
        # Validate indices
        if start_idx >= len(char_col_list) or end_idx >= len(char_col_list):
            return line_coords
        
        # Extract bbox coordinates
        bbox_x_start, bbox_y_start, bbox_x_end, bbox_y_end = line_coords
        
        # Calculate x coordinates using cell indices
        # char_col_list contains cell indices for each character
        # Reference: cell_x_start = bbox_x_start + int(word_col[0] * cell_width)
        #           cell_x_end = bbox_x_start + int((word_col[-1] + 1) * cell_width)
        
        word_x_start = bbox_x_start + int(char_col_list[start_idx] * cell_width - cell_width*2) # - cell width : padding_left
        word_x_end = bbox_x_start + int((char_col_list[end_idx] + 1) * cell_width + cell_width*2) # + cell_width : padding_wright
        
        # Return as [x1, y1, x2, y2]
        result = [int(word_x_start), int(bbox_y_start), int(word_x_end), int(bbox_y_end)]
        return result
    
    def split_line_by_line(self, output_list, thresh_pix=8, num_line_chunks=3):
        '''
        output_list:
        [
            {
                'coords': box,
                'score': score,
                'text': text,
                'char_list': cl,
                'char_col_list': ccl,
                'cell_width': cw,
                'char_boxes': char_boxes,
                'center_row_pos': center_row_pos,
            }
            ...
        ]
        '''
        lines = []
        line_chunks = []
        cur_pos = 0
        len_line_chunks = 0
        for item in output_list:
            center_row_pos = item['center_row_pos']
            if abs(center_row_pos - cur_pos) > thresh_pix:
                cur_pos = center_row_pos
                len_line_chunks += 1
                if num_line_chunks < len_line_chunks:
                    lines.append(line_chunks)
                    line_chunks = []
                    len_line_chunks = 0
                    
            line_chunks.append(item)
        if line_chunks:
            lines.append(line_chunks)
        
        
        result_lines = []
        for line in lines:
            char_list = []
            char_boxes = []
            
            line_text = ''.join([l['text'] for l in line])
            [char_list.extend(l['char_list']) for l in line]
            [char_boxes.extend(l['char_boxes']) for l in line]
            
            result_lines.append({
                'text': line_text,
                'char_list': char_list,
                'char_boxes': char_boxes,
            })
        
        return result_lines


    def find_word_bbox(self, full_text, find_text, char_boxes):
        start_idx = full_text.find(find_text)
        end_idx = start_idx + len(find_text) - 1
        if start_idx == -1 or end_idx == -1:
            return []
        result_boxes = np.array(char_boxes[start_idx:end_idx+1])
        min_x = np.min(result_boxes[:, 0])
        min_y = np.min(result_boxes[:, 1])
        max_x = np.max(result_boxes[:, 2])
        max_y = np.max(result_boxes[:, 3])
        result_box = np.array([min_x, min_y, max_x, max_y])
        return result_box.tolist()