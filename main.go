package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		totalBurst        int64
		newBurstTimes     []int64
		currentProcessNum int64
		currentBurstTime  int64
	)

	for i := range processes {
		totalBurst += int64(processes[i].BurstDuration)                   //determines total loop count based on total time
		newBurstTimes = append(newBurstTimes, processes[i].BurstDuration) // creates a new and copies the original burst times
	}

	for i := 0; i <= int(totalBurst); i++ {
		serviceTime = int64(i)
		var currentHighestPriorityValue = 99999
		var shortestTime = 99999
		lastProcessNum := currentProcessNum // set last process before current process gets updated to a new process

		//checks the processes arrival time and priority
		for j := range processes {
			if processes[j].ArrivalTime <= int64(i) && processes[j].Priority < int64(currentHighestPriorityValue) && newBurstTimes[j] > 0 {
				currentProcessNum = int64(j)
				currentHighestPriorityValue = int(processes[j].Priority)
				shortestTime = int(newBurstTimes[j])
			}
			if processes[j].ArrivalTime == int64(i) && processes[j].Priority < int64(currentHighestPriorityValue) && newBurstTimes[j] > 0 && newBurstTimes[j] < int64(shortestTime) {
				currentProcessNum = int64(j)
				currentHighestPriorityValue = int(processes[j].Priority)
				shortestTime = int(newBurstTimes[j])
			}
		}

		if lastProcessNum != currentProcessNum {
			currentBurstTime = 0
		}

		newBurstTimes[currentProcessNum] -= 1
		currentBurstTime += 1

		//for completed processes
		if newBurstTimes[currentProcessNum] == 0 {
			waitingTime = serviceTime - (processes[currentProcessNum].ArrivalTime + currentBurstTime) + 1
			//waitingTime = serviceTime - (processes[currentProcessNum].ArrivalTime + processes[currentProcessNum].BurstDuration - newBurstTimes[currentProcessNum]) + 1

			totalWait += float64(waitingTime)

			start := waitingTime + processes[currentProcessNum].ArrivalTime

			turnaround := serviceTime + 1 - processes[currentProcessNum].ArrivalTime
			totalTurnaround += float64(turnaround)

			completion := serviceTime + 1
			lastCompletion = float64(completion)

			schedule[currentProcessNum] = []string{
				fmt.Sprint(processes[currentProcessNum].ProcessID),
				fmt.Sprint(processes[currentProcessNum].Priority),
				fmt.Sprint(processes[currentProcessNum].BurstDuration),
				fmt.Sprint(processes[currentProcessNum].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[currentProcessNum].ProcessID,
				Start: start,
				Stop:  serviceTime + 1,
			})
		}

		//for processes that were preempted
		if newBurstTimes[lastProcessNum] != 0 && lastProcessNum != currentProcessNum {
			start := serviceTime - (processes[lastProcessNum].BurstDuration - newBurstTimes[lastProcessNum])

			gantt = append(gantt, TimeSlice{
				PID:   processes[lastProcessNum].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		totalBurst        int64
		newBurstTimes     []int64
		currentProcessNum int64
		currentBurstTime  int64
	)

	for i := range processes {
		totalBurst += int64(processes[i].BurstDuration)                   //determines total loop count based on total time
		newBurstTimes = append(newBurstTimes, processes[i].BurstDuration) // creates a new and copies the original burst times
	}

	for i := 0; i <= int(totalBurst); i++ {
		serviceTime = int64(i)
		var shortestTime = 99999
		lastProcessNum := currentProcessNum // set last process before current process gets updated to a new process

		//checks arrival time and burst duration
		for j := range processes {
			if processes[j].ArrivalTime <= int64(i) && newBurstTimes[j] < int64(shortestTime) && newBurstTimes[j] > 0 {
				currentProcessNum = int64(j)
				shortestTime = int(newBurstTimes[j])
			}
		}

		//resets burst time for new processes
		if lastProcessNum != currentProcessNum {
			currentBurstTime = 0
		}

		newBurstTimes[currentProcessNum] -= 1
		currentBurstTime += 1

		//for completed processes
		if newBurstTimes[currentProcessNum] == 0 {
			waitingTime = serviceTime - (processes[currentProcessNum].ArrivalTime + currentBurstTime) + 1

			totalWait += float64(waitingTime)

			start := waitingTime + processes[currentProcessNum].ArrivalTime

			turnaround := serviceTime + 1 - processes[currentProcessNum].ArrivalTime
			totalTurnaround += float64(turnaround)

			completion := serviceTime + 1
			lastCompletion = float64(completion)

			schedule[currentProcessNum] = []string{
				fmt.Sprint(processes[currentProcessNum].ProcessID),
				fmt.Sprint(processes[currentProcessNum].Priority),
				fmt.Sprint(processes[currentProcessNum].BurstDuration),
				fmt.Sprint(processes[currentProcessNum].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[currentProcessNum].ProcessID,
				Start: start,
				Stop:  serviceTime + 1,
			})
		}

		//for processes that were preempted
		if newBurstTimes[lastProcessNum] != 0 && lastProcessNum != currentProcessNum {
			start := serviceTime - (processes[lastProcessNum].BurstDuration - newBurstTimes[lastProcessNum])

			gantt = append(gantt, TimeSlice{
				PID:   processes[lastProcessNum].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		totalBurst        int64
		newBurstTimes     []int64
		currentProcessNum int64
		currentBurstTime  int64
		//lastBurstTime      int64
		TIMESLICE = int64(4)
	)

	for i := range processes {
		totalBurst += int64(processes[i].BurstDuration)                   //determines total loop count based on total time
		newBurstTimes = append(newBurstTimes, processes[i].BurstDuration) // creates a new and copies the original burst times
	}

	for i := 0; i < int(totalBurst); i++ {
		serviceTime = int64(i)

		lastProcessNum := currentProcessNum // set last process before current process gets updated to a new process

		// switch processes if time is up or if the process finished
		if currentBurstTime == TIMESLICE || newBurstTimes[currentProcessNum] == 0 {
			currentProcessNum += 1                          // increments process number to move to next process
			if currentProcessNum >= int64(len(processes)) { // checks to see if the number for the current process is larger than num processes
				currentProcessNum = 0 // sets number to 0 to go back to start, makes it so its like a circular queue without actually making one
			}
			//lastBurstTime = currentBurstTime; idk why it hates my variable declarations
			currentBurstTime = 0 // resets current burst time to 0 since its a new process

			//checks if the new process is already done, if it is go to the next one
			for newBurstTimes[currentProcessNum] == 0 {
				currentProcessNum += 1
				if currentProcessNum >= int64(len(processes)) { // checks to see if the number for the current process is larger than num processes
					currentProcessNum = 0 // sets number to 0 to go back to start, makes it so its like a circular queue without actually making one
				}
			}

		}

		newBurstTimes[currentProcessNum] -= 1
		currentBurstTime += 1

		//for processes that were preempted
		if newBurstTimes[lastProcessNum] != 0 && lastProcessNum != currentProcessNum {
			//start := serviceTime - (processes[lastProcessNum].BurstDuration - newBurstTimes[lastProcessNum])
			//start := serviceTime - lastBurstTime
			start := serviceTime - TIMESLICE

			gantt = append(gantt, TimeSlice{
				PID:   processes[lastProcessNum].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
		}

		//for completed processes
		if newBurstTimes[currentProcessNum] == 0 {
			waitingTime = serviceTime - (processes[currentProcessNum].ArrivalTime + currentBurstTime) + 1

			totalWait += float64(waitingTime)

			start := waitingTime + processes[currentProcessNum].ArrivalTime

			turnaround := serviceTime + 1 - processes[currentProcessNum].ArrivalTime
			totalTurnaround += float64(turnaround)

			completion := serviceTime + 1
			lastCompletion = float64(completion)

			schedule[currentProcessNum] = []string{
				fmt.Sprint(processes[currentProcessNum].ProcessID),
				fmt.Sprint(processes[currentProcessNum].Priority),
				fmt.Sprint(processes[currentProcessNum].BurstDuration),
				fmt.Sprint(processes[currentProcessNum].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[currentProcessNum].ProcessID,
				Start: start,
				Stop:  serviceTime + 1,
			})
		}

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
