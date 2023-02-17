import { ComponentMeta, ComponentStory } from '@storybook/react'
import { Collapse } from 'antd'
import { LemonCollapse as LemonCollapseComponent } from './LemonCollapse'

export default {
    title: 'Lemon UI/Lemon Collapse',
    component: LemonCollapseComponent,
} as ComponentMeta<typeof LemonCollapseComponent>

const Template: ComponentStory<typeof LemonCollapseComponent> = (props) => {
    return (
        <div className="flex flex-col gap-4">
            <LemonCollapseComponent {...props}>
                <LemonCollapseComponent.Panel header="Panel 1">
                    <p>Panel 1 content</p>
                </LemonCollapseComponent.Panel>
                <LemonCollapseComponent.Panel header="Panel 2">
                    <p>Panel 2 content</p>
                </LemonCollapseComponent.Panel>
            </LemonCollapseComponent>

            <Collapse>
                <Collapse.Panel header="Panel 1" key="1">
                    <p>Panel 1 content</p>
                </Collapse.Panel>
                <Collapse.Panel header="Panel 2" key="2">
                    <p>Panel 2 content</p>
                </Collapse.Panel>
            </Collapse>
        </div>
    )
}

export const LemonCollapse = Template.bind({})
LemonCollapse.args = {}
