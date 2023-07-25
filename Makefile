SRC = src/

test: main.cpp $(SRC)*.hpp
	g++ main.cpp -std=c++17 -fopenmp -march=native -O3 -w -o z_run_test.out

execute: z_run_test.out
	./z_run_test.out

compile_and_execute: main.cpp $(SRC)*.hpp
	g++ main.cpp -std=c++17 -fopenmp -march=native -O3 -w -o z_run_test.out && ./z_run_test.out

clean:
	rm *.out