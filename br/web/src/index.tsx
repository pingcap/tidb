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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import CssBaseline from '@material-ui/core/CssBaseline';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import * as React from 'react';
import { render } from 'react-dom';
import { BrowserRouter, Redirect, Route, Switch } from 'react-router-dom';

import * as api from './api';
import InfoPage from './InfoPage';
import ProgressPage from './ProgressPage';
import TableProgressPage from './TableProgressPage';
import TitleBar from './TitleBar';


const styles = (theme: Theme) => createStyles({
    toolbar: theme.mixins.toolbar,
});

interface Props extends WithStyles<typeof styles> {
}

interface State {
    taskQueue: api.TaskQueue,
    taskProgress: api.TaskProgress,
    hasActiveTableName: boolean,
    activeTableName: string,
    activeTableProgress: api.TableProgress,
    paused: boolean,
}

class App extends React.Component<Props, State> {
    constructor(props: Props) {
        super(props);

        this.state = {
            taskQueue: {
                current: null,
                queue: [],
            },
            taskProgress: {
                s: api.TaskStatus.NotStarted,
                t: {},
                m: undefined,
            },
            hasActiveTableName: false,
            activeTableName: '',
            activeTableProgress: api.EMPTY_TABLE_PROGRESS,
            paused: false,
        };
    }

    handleRefresh = async () => {
        const [taskQueue, taskProgress, paused, activeTableProgress] = await Promise.all([
            api.fetchTaskQueue(),
            api.fetchTaskProgress(),
            api.fetchPaused(),

            // not sure if it's safe to do this...
            // but we can't use `setState(states => ...)` due to the `await`
            this.state.hasActiveTableName ?
                api.fetchTableProgress(this.state.activeTableName).catch(() => api.EMPTY_TABLE_PROGRESS) :
                Promise.resolve(api.EMPTY_TABLE_PROGRESS),
        ]);
        this.setState({ taskQueue, taskProgress, paused, activeTableProgress });
    }

    handleTogglePaused = () => {
        this.setState((state: Readonly<State>) => {
            if (state.paused) {
                api.resume();
            } else {
                api.pause();
            }
            return { paused: !state.paused };
        });
    }

    handleSubmitTask = async (taskCfg: string) => {
        await api.submitTask(taskCfg);
        setTimeout(this.handleRefresh, 500);
    }

    handleDeleteTask = async (taskID: api.TaskID) => {
        await api.deleteTask(taskID);
        setTimeout(this.handleRefresh, 500);
    }

    handleMoveTaskToFront = async (taskID: api.TaskID) => {
        await api.moveTaskToFront(taskID);
        this.setState({ taskQueue: await api.fetchTaskQueue() });
    }

    handleMoveTaskToBack = async (taskID: api.TaskID) => {
        await api.moveTaskToBack(taskID);
        this.setState({ taskQueue: await api.fetchTaskQueue() });
    }

    handleChangeActiveTableProgress = async (tableName?: string) => {
        let shouldRefresh = false;
        this.setState(
            state => {
                shouldRefresh = tableName !== state.activeTableName;
                return { hasActiveTableName: false }
            },
            async () => {
                if (!shouldRefresh || !tableName) {
                    return;
                }
                const tableProgress = await api.fetchTableProgress(tableName);
                this.setState({
                    hasActiveTableName: true,
                    activeTableName: tableName,
                    activeTableProgress: tableProgress,
                });
            },
        );
    }

    render() {
        const { classes } = this.props;
        return (
            <BrowserRouter basename='/web'>
                <CssBaseline />
                <TitleBar
                    taskQueue={this.state.taskQueue}
                    taskProgress={this.state.taskProgress}
                    paused={this.state.paused}
                    onRefresh={this.handleRefresh}
                    onSubmitTask={this.handleSubmitTask}
                    onTogglePaused={this.handleTogglePaused}
                />
                <main>
                    <div className={classes.toolbar} />
                    <Switch>
                        <Route path='/progress'>
                            <ProgressPage
                                taskProgress={this.state.taskProgress}
                            />
                        </Route>
                        <Route path='/tasks'>
                            <InfoPage
                                taskQueue={this.state.taskQueue}
                                getTaskCfg={api.fetchTaskCfg}
                                onDelete={this.handleDeleteTask}
                                onMoveToFront={this.handleMoveTaskToFront}
                                onMoveToBack={this.handleMoveTaskToBack}
                            />
                        </Route>
                        <Route path='/table'>
                            {({ location }) => <TableProgressPage
                                tableName={decodeURIComponent(location.search.substr(3))}
                                tableProgress={this.state.activeTableProgress}
                                onChangeActiveTableProgress={this.handleChangeActiveTableProgress}
                            />}
                        </Route>
                        <Redirect to='/progress' />
                    </Switch>
                </main>
            </BrowserRouter>
        );
    }
}

const StyledApp = withStyles(styles)(App);

render(<StyledApp />, document.getElementById('app'));

