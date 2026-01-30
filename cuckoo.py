from manim import *

class CuckooHashing(Scene):
    def construct(self):
        # Configuration
        TABLE_SIZE = 8
        CELL_WIDTH = 1.0
        CELL_HEIGHT = 0.9
        T1_POS = DOWN * 0.2 + LEFT * 3.5  # Table 1 position (centered, moved lower)
        T2_POS = DOWN * 2.9 + LEFT * 3.5  # Table 2 position (centered, larger gap)

        # --- Helper Functions ---

        def create_table(title_text, pos):
            """Create a hash table with cells and indices"""
            squares = VGroup()
            indices = VGroup()
            
            for i in range(TABLE_SIZE):
                # The cell square
                sq = Rectangle(width=CELL_WIDTH, height=CELL_HEIGHT)
                sq.move_to(pos + RIGHT * i * CELL_WIDTH)
                squares.add(sq)
                
                # The index number (above the cell)
                idx = Text(str(i), font_size=16).next_to(sq, UP, buff=0.15)
                indices.add(idx)
            
            # Title above the table
            title = Text(title_text, font_size=22, color=BLUE, weight=BOLD)
            title.next_to(squares, UP, buff=0.4)
            
            table_group = VGroup(title, squares, indices)
            return table_group, squares

        # --- Scene Construction ---

        # 1. Main Title
        main_title = Text("Cuckoo Hashing", font_size=40).to_edge(UP)
        self.play(Write(main_title))
        self.wait(0.5)

        # 2. Hash Function Explanation
        hash_explanation = VGroup(
            Text("Hash Function 1: h1(key) = key % n", font_size=22, color=BLUE),
            Text("Hash Function 2: h2(key) = (key / n) % n", font_size=22, color=GREEN),
            Text(f"Table Size (n) = {TABLE_SIZE}", font_size=20)
        ).arrange(DOWN).shift(UP * 1.3)
        self.play(FadeIn(hash_explanation))
        self.wait(1.2)

        # 3. Algorithm Explanation
        algo_explanation = Text("Two tables alternate kicks until empty spot found", font_size=20, color=YELLOW)
        algo_explanation.next_to(hash_explanation, DOWN, buff=0.3)
        self.play(FadeIn(algo_explanation))
        self.wait(0.8)
        self.play(FadeOut(hash_explanation), FadeOut(algo_explanation))

        # 4. Create Tables
        t1_group, t1_squares = create_table("T1", T1_POS)
        t2_group, t2_squares = create_table("T2", T2_POS)
        
        self.play(Create(t1_group), Create(t2_group))
        self.wait(0.5)

        # Logical state tracking
        table1_content = [None] * TABLE_SIZE  # (key, mobject) or None
        table2_content = [None] * TABLE_SIZE  # (key, mobject) or None

        # --- Insertion Logic ---

        def h1(key):
            return key & (TABLE_SIZE - 1)

        def h2(key):
            return (key // TABLE_SIZE) & (TABLE_SIZE - 1)

        def animate_insert(key):
            """Insert a key with animation of kicks between tables"""
            h1_val = h1(key)
            h2_val = h2(key)
            
            # Show insertion info
            insert_label = Text(
                f"Insert {key}: h1({key})={h1_val}, h2({key})={h2_val}",
                font_size=20, color=BLUE, weight=BOLD
            )
            insert_label.to_edge(UP).shift(DOWN * 0.6)
            self.play(Write(insert_label))
            self.wait(0.3)

            # Create value mobject
            val_mob = Text(str(key), font_size=28, color=BLUE, weight=BOLD)
            val_mob.move_to(T1_POS + RIGHT * h1_val * CELL_WIDTH + UP * 2.0)
            
            self.play(FadeIn(val_mob))
            
            # Insertion process
            current_key = key
            current_mob = val_mob
            table_id = 1  # Start with table 1
            max_iterations = 20  # Prevent infinite loops
            iteration = 0
            
            while iteration < max_iterations:
                iteration += 1
                
                if table_id == 1:
                    target_idx = h1(current_key)
                    target_squares = t1_squares
                    target_table = table1_content
                    target_pos = T1_POS
                    next_table_id = 2
                    other_table = table2_content
                    other_squares = t2_squares
                    other_pos = T2_POS
                else:
                    target_idx = h2(current_key)
                    target_squares = t2_squares
                    target_table = table2_content
                    target_pos = T2_POS
                    next_table_id = 1
                    other_table = table1_content
                    other_squares = t1_squares
                    other_pos = T1_POS

                target_sq = target_squares[target_idx]
                
                # Highlight target cell
                self.play(target_sq.animate.set_stroke(YELLOW, width=5), run_time=0.25)
                
                # Move value to target cell
                self.play(current_mob.animate.move_to(target_sq.get_center()), run_time=0.5)
                self.wait(0.2)

                # Check if cell is empty
                if target_table[target_idx] is None:
                    # Empty! Place and finish
                    target_table[target_idx] = (current_key, current_mob)
                    
                    placement_msg = Text(
                        f"✓ Placed {current_key} in T{table_id}[{target_idx}]",
                        font_size=18, color=GREEN
                    )
                    placement_msg.next_to(insert_label, DOWN, buff=0.2)
                    self.play(FadeIn(placement_msg))
                    self.wait(0.4)
                    self.play(FadeOut(placement_msg))
                    
                    self.play(target_sq.animate.set_stroke(WHITE, width=2))
                    break
                
                else:
                    # Collision! Show and kick
                    existing_key, existing_mob = target_table[target_idx]
                    
                    collision_msg = VGroup(
                        Text(f"Collision! T{table_id}[{target_idx}] occupied", font_size=18, color=RED),
                        Text(f"Current: {current_key}, Existing: {existing_key}", font_size=16, color=RED)
                    ).arrange(DOWN).next_to(insert_label, DOWN, buff=0.2)
                    
                    self.play(FadeIn(collision_msg))
                    self.wait(0.4)
                    
                    # Kick animation
                    kick_text = Text("KICK!", font_size=24, color=RED, weight=BOLD)
                    kick_text.move_to(target_sq.get_corner(UR) + RIGHT * 0.3)
                    self.play(FadeIn(kick_text, scale=1.5), run_time=0.8)
                    self.play(FadeOut(kick_text), run_time=0.8)
                    
                    # Swap: new value takes this spot, old value gets kicked
                    target_table[target_idx] = (current_key, current_mob)
                    
                    # Calculate hash values for the kicked element
                    kicked_h1 = h1(existing_key)
                    kicked_h2 = h2(existing_key)
                    next_hash = kicked_h1 if next_table_id == 1 else kicked_h2
                    
                    # Show what's about to happen
                    kick_info = Text(
                        f"Kicking {existing_key} to T{next_table_id}, h{next_table_id}({existing_key})={next_hash}",
                        font_size=16, color=YELLOW
                    ).next_to(collision_msg, DOWN, buff=0.2)
                    self.play(FadeIn(kick_info))
                    self.wait(0.3)
                    
                    # Move kicked element ABOVE its target position (between tables, above target hash cell)
                    next_target_idx = next_hash
                    if next_table_id == 1:
                        next_pos = T1_POS + RIGHT * next_target_idx * CELL_WIDTH
                    else:
                        next_pos = T2_POS + RIGHT * next_target_idx * CELL_WIDTH
                    
                    # Position above the target (in the gap space)
                    above_pos = next_pos + UP * 1.5
                    self.play(existing_mob.animate.move_to(above_pos).set_color(YELLOW), run_time=0.5)
                    self.wait(0.2)
                    
                    # Prepare to insert the kicked element into the other table
                    current_key = existing_key
                    current_mob = existing_mob
                    table_id = next_table_id
                    
                    self.play(FadeOut(collision_msg), FadeOut(kick_info))
                    self.play(target_sq.animate.set_stroke(WHITE, width=2))
            
            self.play(FadeOut(insert_label))
            self.wait(0.6)

        # --- Sequence of Insertions ---
        # Carefully chosen keys to demonstrate cascading kicks:
        # With h1(x) = x & 7 and h2(x) = (x / 8) & 7
        # 9:  h1=1, h2=1
        # 17: h1=1, h2=2
        # 10: h1=2, h2=1
        # 18: h1=2, h2=2 (causes cascading kicks)
        
        # 1. Insert 9: Simple insertion into T1[1]
        animate_insert(9)
        
        # 2. Insert 17: Collision at T1[1], kicks 9 to T2[1]
        animate_insert(17)
        
        # 3. Insert 10: Simple insertion into T1[2]
        animate_insert(10)
        
        # 4. Insert 18: Creates cascading kicks!
        # T1[2] occupied by 10 -> kick to T2[1] occupied by 9 -> kick to T1[1] occupied by 17
        # -> kick to T2[2] (empty)
        animate_insert(18)

        self.wait(2)