from manim import *

class HopscotchHashing(Scene):
    def construct(self):
        # Configuration
        N = 10
        H = 3
        CELL_WIDTH = 1.0
        CELL_HEIGHT = 1.0
        START_POS = LEFT * 4.5 + DOWN * 0.5
        
        # Data Structures (Logical State)
        # table: stores (key, char_label, Mobject) or None
        table = [None] * N
        # bitmaps: list of sets, where bitmaps[i] contains offsets {0, 1, 2}
        bitmaps = [set() for _ in range(N)] 

        # --- Visual Construction Helpers ---
        
        def create_table_visuals():
            cells = VGroup()
            indices = VGroup()
            bitmap_indicators = VGroup()
            
            for i in range(N):
                # Main Cell
                pos = START_POS + RIGHT * i * CELL_WIDTH
                sq = Square(side_length=CELL_WIDTH).move_to(pos)
                cells.add(sq)
                
                # Index Number
                idx_text = Text(str(i), font_size=16).next_to(sq, DOWN, buff=0.1)
                indices.add(idx_text)
                
                # Bitmap (H small dots/squares below index)
                bm_group = VGroup()
                for b in range(H):
                    dot = Square(side_length=0.15, stroke_width=1, fill_opacity=0.2, color=GRAY)
                    # Position small squares in a row
                    dot.move_to(idx_text.get_center() + DOWN * 0.3 + RIGHT * ((b - (H-1)/2) * 0.2))
                    bm_group.add(dot)
                bitmap_indicators.add(bm_group)
            
            return cells, indices, bitmap_indicators

        # Create Scene Objects
        title = Text(f"Hopscotch Hashing (N={N}, H={H})", font_size=36).to_edge(UP)
        cells, indices, bitmap_indicators = create_table_visuals()
        
        table_group = VGroup(cells, indices, bitmap_indicators)
        
        self.play(Write(title))
        self.play(Create(table_group))
        self.wait(0.5)

        # --- Logic & Animation Functions ---

        def update_bitmap_visuals(hash_idx):
            """Refreshes the bitmap dots for a specific hash bucket"""
            group = bitmap_indicators[hash_idx]
            anims = []
            for offset in range(H):
                dot = group[offset]
                if offset in bitmaps[hash_idx]:
                    anims.append(dot.animate.set_fill(GREEN, opacity=1).set_stroke(GREEN))
                else:
                    anims.append(dot.animate.set_fill(GRAY, opacity=0.2).set_stroke(GRAY))
            return anims

        def get_distance(idx, hash_val):
            return (idx - hash_val + N) % N

        def animate_insertion(key, char_label):
            hash_val = key % N
            
            # 1. Announce Insertion
            info_text = MarkupText(
                f"Insert <b>'{char_label}'</b> (Hash <b>{hash_val}</b>)", 
                font_size=24, color=YELLOW
            ).next_to(title, DOWN)
            self.play(FadeIn(info_text))
            
            # Create the object text
            val_mob = Text(char_label, font_size=32, weight=BOLD, color=WHITE)
            val_mob.move_to(cells[hash_val].get_center() + UP * 1.5)
            self.play(FadeIn(val_mob))

            # 2. Linear Probing for Empty Slot
            curr = hash_val
            path_searched = []
            
            # Visual probe indicator
            probe_rect = SurroundingRectangle(cells[curr], color=ORANGE, buff=0.05)
            self.play(Create(probe_rect), run_time=0.3)
            
            while table[curr] is not None:
                path_searched.append(curr)
                curr = (curr + 1) % N
                # Move probe rect
                self.play(probe_rect.animate.move_to(cells[curr]), run_time=0.2)
                
                if curr == hash_val: # Table full check (not happening in this example)
                    break
            
            empty_idx = curr
            self.play(probe_rect.animate.set_color(GREEN), run_time=0.2)
            self.wait(0.2)
            self.play(FadeOut(probe_rect))

            # 3. Hopscotch Logic (Displacement)
            # While the empty spot is too far from the home bucket...
            while get_distance(empty_idx, hash_val) >= H:
                dist = get_distance(empty_idx, hash_val)
                
                # Visual warning
                warning = Text(f"Distance {dist} >= {H}. Must displace.", font_size=20, color=RED)
                warning.next_to(table_group, UP, buff=0.5)
                self.play(FadeIn(warning))
                self.wait(0.5)
                
                # Search backwards from empty_idx (H-1 down to 1)
                # This matches your C++ logic: for(offset = H-1; offset > 0; --offset)
                found_swap = False
                swap_candidate_idx = -1
                
                for offset in range(H - 1, 0, -1):
                    check_idx = (empty_idx - offset + N) % N
                    
                    # Highlight checking
                    checker = SurroundingRectangle(cells[check_idx], color=BLUE, buff=0.05)
                    self.play(Create(checker), run_time=0.1)
                    
                    entry = table[check_idx]
                    if entry is not None:
                        e_key, e_char, e_mob = entry
                        e_hash = e_key % N
                        
                        # Can this element move to empty_idx?
                        # It can move if dist(new_pos, home_hash) < H
                        new_dist = get_distance(empty_idx, e_hash)
                        
                        if new_dist < H:
                            # FOUND A VALID SWAP
                            swap_candidate_idx = check_idx
                            
                            success_text = Text("Found!", color=GREEN, font_size=18).next_to(checker, UP)
                            self.play(Write(success_text))
                            self.wait(0.3)
                            self.play(FadeOut(checker), FadeOut(success_text))
                            found_swap = True
                            break
                    
                    self.play(FadeOut(checker), run_time=0.1)

                if found_swap:
                    # Perform the swap logically and visually
                    src_idx = swap_candidate_idx
                    dst_idx = empty_idx
                    
                    s_key, s_char, s_mob = table[src_idx]
                    s_hash = s_key % N
                    
                    # Animate move
                    self.play(s_mob.animate.move_to(cells[dst_idx].get_center()))
                    
                    # Update State
                    table[dst_idx] = table[src_idx]
                    table[src_idx] = None
                    
                    # Update Bitmaps
                    # Remove old offset from source hash
                    old_offset_val = get_distance(src_idx, s_hash)
                    bitmaps[s_hash].remove(old_offset_val)
                    
                    # Add new offset to source hash
                    new_offset_val = get_distance(dst_idx, s_hash)
                    bitmaps[s_hash].add(new_offset_val)
                    
                    # Animate Bitmap Update
                    self.play(*update_bitmap_visuals(s_hash))
                    
                    # Update loop variable
                    empty_idx = src_idx
                    self.play(FadeOut(warning))
                else:
                    # Should not happen in this specific example
                    fail = Text("Resize Needed!", color=RED).to_edge(CENTER)
                    self.play(FadeIn(fail))
                    self.wait()
                    return

            # 4. Final Placement
            # Move the new value into the now-valid empty_idx
            self.play(val_mob.animate.move_to(cells[empty_idx].get_center()))
            
            # Update Logic
            table[empty_idx] = (key, char_label, val_mob)
            final_offset = get_distance(empty_idx, hash_val)
            bitmaps[hash_val].add(final_offset)
            
            # Update Bitmap Visuals
            self.play(*update_bitmap_visuals(hash_val))
            
            self.play(FadeOut(info_text))
            self.wait(0.5)

        # --- Execute the Sequence from the Example ---
        # Sequence: a(7), b(8), l(8), d(0), e(1), k(7)
        data = [
            (7, "a"),
            (8, "b"),
            (8, "l"), # Collides with b, should go to 9
            (0, "d"),
            (1, "e"),
            (7, "k")  # The complex case
        ]

        for k, c in data:
            animate_insertion(k, c)

        self.wait(2)