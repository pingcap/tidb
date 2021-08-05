// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

import BigNumber from 'bignumber.js';
import * as JSONBigInt from 'json-bigint';


export type TaskID = BigNumber | number;

export function dateFromTaskID(taskID: TaskID): Date {
    return new Date((taskID as any) * 1e-6);
}

export enum TaskStatus {
    NotStarted = 0,
    Running = 1,
    Completed = 2,
}

export enum CheckpointStatus {
    Missing = 0,
    MaxInvalid = 25,
    Loaded = 30,
    AllWritten = 60,
    Closed = 90,
    Imported = 120,
    IndexImported = 140,
    AlteredAutoInc = 150,
    ChecksumSkipped = 170,
    Checksummed = 180,
    AnalyzeSkipped = 200,
    Analyzed = 210,

    LoadErrored = 3,
    WriteErrored = 6,
    CloseErrored = 9,
    ImportErrored = 12,
    IndexImportErrored = 14,
    AlterAutoIncErrored = 15,
    ChecksumErrored = 18,
    AnalyzeErrored = 21,
}

export interface TableInfo {
    w: number
    z: number
    s: TaskStatus
    m?: string
}

export interface TaskProgress {
    s: TaskStatus
    t: { [tableName: string]: TableInfo }
    m?: string
}

export interface TaskQueue {
    current: TaskID | null
    queue: TaskID[]
}

export interface ChunkProgress {
    Key: {
        Path: string,
        Offset: number,
    }
    ColumnPermutation: number[]
    Chunk: {
        Offset: number,
        EndOffset: number,
        PrevRowIDMax: number,
        RowIDMax: number,
    }
    Checksum: {
        checksum: number,
        size: number,
        kvs: number,
    }
}

export interface EngineProgress {
    Status: CheckpointStatus
    Chunks: ChunkProgress[]
}

export interface TableProgress {
    Status: CheckpointStatus
    AllocBase: number
    Engines: { [engineID: string]: EngineProgress }
}

export const EMPTY_TABLE_PROGRESS: TableProgress = {
    Status: CheckpointStatus.Missing,
    AllocBase: 0,
    Engines: {},
}

export function classNameOfStatus(
    status: { s: TaskStatus, m?: string },
    notStarted: string,
    running: string,
    succeed: string,
    failed: string,
): string {
    switch (status.s) {
        case TaskStatus.NotStarted:
            return notStarted;
        case TaskStatus.Running:
            return running;
        case TaskStatus.Completed:
            return status.m ? failed : succeed;
    }
}

export function labelOfCheckpointStatus(status: CheckpointStatus): string {
    switch (status) {
        case CheckpointStatus.Missing:
            return "missing";

        case CheckpointStatus.Loaded:
            return "writing";
        case CheckpointStatus.AllWritten:
            return "closing";
        case CheckpointStatus.Closed:
            return "importing";
        case CheckpointStatus.Imported:
            return "imported";
        case CheckpointStatus.IndexImported:
            return "index imported";
        case CheckpointStatus.AlteredAutoInc:
            return "doing checksum";
        case CheckpointStatus.Checksummed:
        case CheckpointStatus.ChecksumSkipped:
            return "analyzing";
        case CheckpointStatus.Analyzed:
        case CheckpointStatus.AnalyzeSkipped:
            return "finished";

        case CheckpointStatus.LoadErrored:
            return "loading (errored)";
        case CheckpointStatus.WriteErrored:
            return "writing (errored)";
        case CheckpointStatus.CloseErrored:
            return "closing (errored)";
        case CheckpointStatus.ImportErrored:
            return "importing (errored)";
        case CheckpointStatus.IndexImportErrored:
            return "index importing (errored)";
        case CheckpointStatus.AlterAutoIncErrored:
            return "alter auto inc (errored)";
        case CheckpointStatus.ChecksumErrored:
            return "checksum (errored)";
        case CheckpointStatus.AnalyzeErrored:
            return "analyzing (errored)";

        default:
            return "unknown";
    }
}

export const ENGINE_MAX_STEPS = 4;
export const TABLE_MAX_STEPS = 8;

export function stepOfCheckpointStatus(status: CheckpointStatus): number {
    switch (status) {
        case CheckpointStatus.LoadErrored:
            return 0;
        case CheckpointStatus.Loaded:
        case CheckpointStatus.WriteErrored:
            return 1;
        case CheckpointStatus.AllWritten:
        case CheckpointStatus.CloseErrored:
            return 2;
        case CheckpointStatus.Closed:
        case CheckpointStatus.ImportErrored:
            return 3;
        case CheckpointStatus.Imported:
        case CheckpointStatus.IndexImportErrored:
            return 4;
        case CheckpointStatus.IndexImported:
        case CheckpointStatus.AlterAutoIncErrored:
            return 5;
        case CheckpointStatus.AlteredAutoInc:
        case CheckpointStatus.ChecksumErrored:
            return 6;
        case CheckpointStatus.Checksummed:
        case CheckpointStatus.ChecksumSkipped:
        case CheckpointStatus.AnalyzeErrored:
            return 7;
        case CheckpointStatus.Analyzed:
        case CheckpointStatus.AnalyzeSkipped:
            return 8;
        default:
            return 0;
    }
}

export async function fetchTaskQueue(): Promise<TaskQueue> {
    const resp = await fetch('../tasks');
    const text = await resp.text();
    return JSONBigInt.parse(text);
}

export async function fetchTaskProgress(): Promise<TaskProgress> {
    const resp = await fetch('../progress/task');
    return await resp.json();
}

export async function submitTask(taskCfg: string): Promise<void> {
    const resp = await fetch('../tasks', { method: 'POST', body: taskCfg });
    if (resp.ok) {
        return;
    }
    const err = await resp.json();
    throw err.error;
}

export async function fetchPaused(): Promise<boolean> {
    const resp = await fetch('../pause');
    const res = await resp.json();
    return res.paused;
}

export async function pause(): Promise<void> {
    await fetch('../pause', { method: 'PUT' });
}

export async function resume(): Promise<void> {
    await fetch('../resume', { method: 'PUT' });
}

export async function fetchTaskCfg(taskID: TaskID): Promise<any> {
    const resp = await fetch('../tasks/' + taskID);
    const text = await resp.text();
    return JSONBigInt.parse(text);
}

export async function deleteTask(taskID: TaskID): Promise<void> {
    await fetch('../tasks/' + taskID, { method: 'DELETE' });
}

export async function moveTaskToFront(taskID: TaskID): Promise<void> {
    await fetch('../tasks/' + taskID + '/front', { method: 'PATCH' });
}

export async function moveTaskToBack(taskID: TaskID): Promise<void> {
    await fetch('../tasks/' + taskID + '/back', { method: 'PATCH' });
}

export async function fetchTableProgress(tableName: string): Promise<TableProgress> {
    const resp = await fetch('../progress/table?t=' + encodeURIComponent(tableName))
    let res = await resp.json();
    if (resp.ok) {
        return res;
    } else {
        throw res.error;
    }
}
