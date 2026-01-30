from manim import *

class RobinHoodHashing(Scene):
    def construct(self):
        # Configuration
        TABLE_SIZE = 8
        CELL_WIDTH = 1.3
        CELL_HEIGHT = 1.1
        START_POS = DOWN * 0.8 + LEFT * 4.5  # Positioned to keep everything visible

        # --- Helper Functions ---
        
        def create_table():
            squares = VGroup()
            indices = VGroup()
            psl_labels = VGroup()
            
            for i in range(TABLE_SIZE):
                # The cell square
                sq = Rectangle(width=CELL_WIDTH, height=CELL_HEIGHT)
                sq.move_to(START_POS + RIGHT * i * CELL_WIDTH)
                squares.add(sq)
                
                # The index number (above the cell)
                idx = Text(str(i), font_size=20).next_to(sq, UP)
                indices.add(idx)
                
                # The PSL label (below the cell for better visibility)
                psl = Text("PSL: -", font_size=14, color=GRAY)
                psl.move_to(sq.get_bottom() + DOWN * 0.35)
                psl_labels.add(psl)

            table_group = VGroup(squares, indices, psl_labels)
            return table_group, squares, psl_labels

        # --- Scene Construction ---

        # 1. Title
        title = Text("Robin Hood Hashing", font_size=36).to_edge(UP)
        self.play(Write(title))
        self.wait(0.5)

        # 2. Hash Function Explanation
        hash_explanation = VGroup(
            Text("Hash Function: index = key % n", font_size=24, color=BLUE),
            Text(f"Table Size (n) = {TABLE_SIZE}", font_size=20)
        ).arrange(DOWN).shift(UP * 1.5)
        self.play(FadeIn(hash_explanation))
        self.wait(1)

        # 3. PSL Explanation
        psl_explanation = Text("PSL = Probe Sequence Length\n(How far from desired position)", font_size=20, color=YELLOW)
        psl_explanation.shift(UP * 0.5)
        self.play(FadeIn(psl_explanation))
        self.wait(0.8)
        self.play(FadeOut(hash_explanation), FadeOut(psl_explanation))

        # 4. Create Table
        table_group, squares, psl_labels = create_table()
        table_content = [None] * TABLE_SIZE # To track logical state
        table_texts = [None] * TABLE_SIZE   # To track Mobjects inside cells
        
        self.play(Create(table_group))
        self.wait(0.5)

        # --- Logic for Insertion Animation ---
        
        def animate_insert(key, hash_val=None):
            if hash_val is None:
                hash_val = key & (TABLE_SIZE - 1)  # Bitwise AND hash function
                
            # Create the Value Object
            val_mob = Text(str(key), font_size=32, color=BLUE, weight=BOLD)
            val_mob.move_to(START_POS + RIGHT * hash_val * CELL_WIDTH + UP * 2.5)
            
            # Label for "Insert X"
            action_label = Text(f"Insert {key}: Hash = {key} % {TABLE_SIZE} = {hash_val}", font_size=20, color=BLUE, weight=BOLD)
            action_label.to_edge(UP).shift(DOWN * 0.6)  # Positioned below title
            
            self.play(FadeIn(val_mob), Write(action_label))
            self.play(val_mob.animate.move_to(squares[hash_val].get_center()), run_time=0.6)

            curr_idx = hash_val
            curr_psl = 0
            
            # Probing Loop
            while True:
                # Highlight current cell
                self.play(squares[curr_idx].animate.set_stroke(YELLOW, width=6), run_time=0.3)
                self.wait(0.2)
                
                # Case 1: Empty Spot
                if table_content[curr_idx] is None:
                    table_content[curr_idx] = (key, curr_psl)
                    table_texts[curr_idx] = val_mob
                    
                    # Update PSL display
                    new_psl_text = Text(f"PSL: {curr_psl}", font_size=14, color=GREEN)
                    new_psl_text.move_to(psl_labels[curr_idx].get_center())
                    
                    placement_msg = Text(f"✓ Placed at index {curr_idx} with PSL={curr_psl}", font_size=18, color=GREEN)
                    placement_msg.next_to(action_label, DOWN, buff=0.3)
                    
                    self.play(
                        val_mob.animate.move_to(squares[curr_idx].get_center()),
                        Transform(psl_labels[curr_idx], new_psl_text),
                        squares[curr_idx].animate.set_stroke(WHITE, width=4),
                        FadeIn(placement_msg)
                    )
                    self.wait(0.5)
                    self.play(FadeOut(placement_msg))
                    break

                # Case 2: Occupied Spot
                else:
                    existing_key, existing_psl = table_content[curr_idx]
                    
                    # Show comparison
                    comparison = VGroup(
                        Text(f"Comparing PSLs:", font_size=18, color=WHITE),
                        Text(f"Current key {key}: PSL = {curr_psl}", font_size=16, color=BLUE),
                        Text(f"Existing key {existing_key}: PSL = {existing_psl}", font_size=16, color=RED)
                    ).arrange(DOWN, aligned_edge=LEFT).next_to(action_label, DOWN, buff=0.3)
                    
                    self.play(FadeIn(comparison))
                    self.wait(0.4)
                    
                    # Compare PSLs
                    if curr_psl > existing_psl:
                        # ROBIN HOOD SWAP!
                        swap_decision = Text(f"✓ SWAP! ({curr_psl} > {existing_psl})\nRobin Hood steals from the rich!", font_size=18, color=RED, weight=BOLD)
                        swap_decision.next_to(comparison, DOWN, buff=0.2)
                        self.play(FadeIn(swap_decision))
                        self.wait(0.5)
                        
                        # Move existing out
                        old_mob = table_texts[curr_idx]
                        self.play(
                            old_mob.animate.shift(DOWN * 1.2), # Move old value down (visible distance)
                            val_mob.animate.move_to(squares[curr_idx].get_center()), # Place new value
                            run_time=0.6
                        )
                        
                        # Update logic: New value takes this spot
                        table_content[curr_idx] = (key, curr_psl)
                        table_texts[curr_idx] = val_mob
                        
                        # Update PSL Text for the new resident
                        new_psl_text = Text(f"PSL: {curr_psl}", font_size=14, color=GREEN)
                        new_psl_text.move_to(psl_labels[curr_idx].get_center())
                        self.play(Transform(psl_labels[curr_idx], new_psl_text))
                        self.wait(0.3)

                        # Now we must insert the OLD value into the next spot
                        key = existing_key
                        curr_psl = existing_psl
                        val_mob = old_mob # The 'active' mob is now the one we kicked out
                        
                        self.play(FadeOut(comparison), FadeOut(swap_decision))
                    else:
                        # No swap - continue probing
                        no_swap_msg = Text(f"✗ No swap. ({curr_psl} ≤ {existing_psl})\nContinue probing...", font_size=16, color=ORANGE)
                        no_swap_msg.next_to(comparison, DOWN, buff=0.2)
                        self.play(FadeIn(no_swap_msg))
                        self.wait(0.3)
                        self.play(FadeOut(comparison), FadeOut(no_swap_msg))

                    # Move to next index (Linear Probing)
                    self.play(squares[curr_idx].animate.set_stroke(WHITE, width=4))
                    curr_idx = (curr_idx + 1) % TABLE_SIZE
                    curr_psl += 1
                    
                    # Animate moving the floating value to the next cell
                    self.play(val_mob.animate.move_to(squares[curr_idx].get_center()), run_time=0.5)

            self.play(FadeOut(action_label))
            self.wait(0.8)

        # --- Sequence of Insertions ---
        # Keys selected to demonstrate Robin Hood hashing kicks
        # All keys hash using: key & 7 (bitwise AND with 7)
        # 16 → 0, 1 → 1, 9 → 1, 2 → 2, 10 → 2, 24 → 0

        # 1. Insert 16: 16 & 7 = 0 (Simple insertion)
        animate_insert(16)

        # 2. Insert 1: 1 & 7 = 1 (Simple insertion)
        animate_insert(1)
        
        # 3. Insert 9: 9 & 7 = 1 (Collision at pos 1, probes to pos 2)
        # 1 is at [1] with PSL 0
        # 9 wants [1] PSL=0. 0 not > 0. Probes to [2]
        # [2] is empty. Place 9 with PSL=1
        animate_insert(9)

        # 4. Insert 2: 2 & 7 = 2 (Collision at pos 2)
        # 9 is at [2] with PSL=1, so 2 wants [2] PSL=0, 0 not > 1, probes to [3]
        # [3] is empty. Place 2 with PSL=1
        animate_insert(2)

        # 5. Insert 10: 10 & 7 = 2 (Collision at pos 2)
        # 9 at [2] PSL=1, 10 wants [2] PSL=0. No swap. Probes to [3]
        # 2 at [3] PSL=1, 10 now PSL=1. No swap. Probes to [4]
        # [4] empty. Insert 10 with PSL=2
        animate_insert(10)

        # 6. Insert 24: 24 & 7 = 0 (THE ROBIN HOOD KICK!)
        # 16 at [0] PSL=0, 24 wants [0] PSL=0. No swap. Probes to [1]
        # 1 at [1] PSL=0, 24 now PSL=1. No swap. Probes to [2]
        # 9 at [2] PSL=1, 24 now PSL=2. 2 > 1, KICK 9!
        # 24 takes [2]. 9 must continue from [3]
        # 2 at [3] PSL=1, 9 now PSL=2. 2 > 1, KICK 2!
        # 9 takes [3]. 2 must continue from [4]
        # 10 at [4] PSL=2, 2 now PSL=3. 3 > 2, KICK 10!
        # 2 takes [4]. 10 must continue from [5]
        # [5] empty. Insert 10 with PSL=3
        animate_insert(24)

        self.wait(2)